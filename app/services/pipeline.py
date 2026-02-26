"""
Pipeline — orchestrates the full evaluation pipeline for a single event.
  L0: Ingestion validation (done at API layer)
  L1: Deduplication guard
  L2: Rules engine
  L3: Context enricher
  L4: AI scorer (Groq + heuristic fallback)
  L5: Decision arbiter
  L6: Dispatcher
"""
import uuid
from datetime import datetime
from typing import Optional

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.schemas import NotificationEventIn, DecisionResult, ReasonStep, DecisionEnum
from app.services.dedup import run_dedup_pipeline
from app.services.rules_engine import evaluate_rules
from app.services.context_enricher import enrich_context
from app.services.ai_scorer import score_with_ai, score_reason_step, ScoringResult, _heuristic_score
from app.services.arbiter import arbitrate
from app.services.dispatcher import dispatch

log = structlog.get_logger()


def _expired_result(event_id: str, event: NotificationEventIn) -> DecisionResult:
    """Return a NEVER decision for expired events."""
    return DecisionResult(
        event_id=event_id,
        user_id=event.user_id,
        decision=DecisionEnum.never,
        score=None,
        reason_chain=[ReasonStep(
            layer="L0-Ingestion",
            check="expiry_check",
            result="NEVER",
            detail=f"Event expired at {event.expires_at} — suppressed on arrival",
        )],
        ai_used=False,
        fallback_used=False,
        processed_at=datetime.utcnow(),
    )


def _dedup_suppressed_result(
    event_id: str, event: NotificationEventIn,
    reason: str, steps: list[ReasonStep]
) -> DecisionResult:
    return DecisionResult(
        event_id=event_id,
        user_id=event.user_id,
        decision=DecisionEnum.never,
        score=None,
        reason_chain=steps,
        ai_used=False,
        fallback_used=False,
        processed_at=datetime.utcnow(),
    )


async def evaluate_notification(
    event: NotificationEventIn,
    db: AsyncSession,
    event_id: Optional[str] = None,
) -> DecisionResult:
    """
    Full evaluation pipeline. Returns DecisionResult with complete reason chain.
    """
    event_id = event_id or str(uuid.uuid4())
    start_time = datetime.utcnow()

    log.info(
        "pipeline.start",
        event_id=event_id,
        user_id=event.user_id,
        event_type=event.event_type,
        priority_hint=event.priority_hint,
    )

    # ── L0: Expiry check ─────────────────────────────────────────
    if event.expires_at:
        now = datetime.utcnow()
        exp = event.expires_at
        # Normalise timezone
        if exp.tzinfo is not None:
            from datetime import timezone
            now = datetime.now(timezone.utc)
        if exp < now:
            log.info("pipeline.expired_on_arrival", event_id=event_id)
            result = _expired_result(event_id, event)
            return result

    # ── L1: Deduplication ─────────────────────────────────────────
    suppress_reason, fingerprint, dedup_steps = await run_dedup_pipeline(event)
    if suppress_reason:
        log.info("pipeline.dedup_suppressed", event_id=event_id, reason=suppress_reason)
        return _dedup_suppressed_result(event_id, event, suppress_reason, dedup_steps)

    # ── L2: Rules engine ──────────────────────────────────────────
    rule_decision, rule_name, rule_steps = await evaluate_rules(event, db)

    # Short-circuit on hard rules (before expensive context + AI)
    if rule_decision in ("now", "never"):
        # Still need a minimal ScoringResult for dispatcher signature
        dummy_score = ScoringResult(
            score=1.0 if rule_decision == "now" else 0.0,
            decision_hint=rule_decision,
            urgency=1.0 if rule_decision == "now" else 0.0,
            engagement=0.5, fatigue_penalty=0.0, recency_bonus=0.5,
            reasoning=f"Hard rule '{rule_name}' applied",
            ai_used=False, fallback_used=False,
        )
        ai_step = ReasonStep(
            layer="L4-AIScorer", check="skipped", result="SKIPPED",
            detail="AI scoring skipped — hard rule already decided"
        )
        from app.services.context_enricher import UserContext
        dummy_ctx = UserContext(user_id=event.user_id)
        decision, scheduled_at, full_chain, override = arbitrate(
            event, rule_decision, rule_name, dummy_score, dummy_ctx,
            rule_steps, dedup_steps, ai_step
        )
        return await dispatch(
            event_id, event, fingerprint, decision, dummy_score.score,
            scheduled_at, full_chain, dummy_score, override, dummy_ctx, db
        )

    # ── L3: Context enrichment ─────────────────────────────────────
    ctx = await enrich_context(event, db)

    # ── L4: AI scoring ────────────────────────────────────────────
    ai_result = await score_with_ai(event, ctx, db=db, event_id=event_id)
    ai_step = score_reason_step(ai_result)

    # ── L5: Decision arbitration ──────────────────────────────────
    decision, scheduled_at, full_chain, override = arbitrate(
        event, rule_decision, rule_name, ai_result, ctx,
        rule_steps, dedup_steps, ai_step
    )

    # ── L6: Dispatch ──────────────────────────────────────────────
    result = await dispatch(
        event_id, event, fingerprint, decision, ai_result.score,
        scheduled_at, full_chain, ai_result, override, ctx, db
    )

    elapsed_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
    log.info(
        "pipeline.complete",
        event_id=event_id,
        decision=decision.value,
        score=ai_result.score,
        elapsed_ms=round(elapsed_ms, 1),
        ai_used=ai_result.ai_used,
    )

    return result