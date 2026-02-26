"""
Dispatcher — final routing layer.
  NOW    → Kafka send_now_queue + increment fatigue counters
  LATER  → Kafka defer_queue + DigestBatch record
  NEVER  → Audit log only
All decisions are written to audit_log and notification_events.
"""
import json
from datetime import datetime
from typing import Optional

import structlog
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.config import get_settings
from app.models.schemas import DecisionEnum, NotificationEventIn, ReasonStep, DecisionResult
from app.models.tables import NotificationEvent, AuditLog, DigestBatch
from app.services.context_enricher import UserContext
from app.services.ai_scorer import ScoringResult
from app.services.dedup import register_cooldown
from app.utils.redis_client import get_redis, key_count_1h, key_count_24h, key_last_send
from app.utils.kafka_client import publish

log = structlog.get_logger()
settings = get_settings()

# Rolling window helpers (sliding window via INCR + EXPIRE on first increment)
_1H_SECONDS = 3600
_24H_SECONDS = 86400


async def _increment_fatigue_counters(user_id: str, event_type: str):
    """Increment sliding-window send counters in Redis."""
    r = await get_redis()
    try:
        pipe = r.pipeline()
        k1h = key_count_1h(user_id)
        k24h = key_count_24h(user_id)

        await pipe.incr(k1h)
        await pipe.expire(k1h, _1H_SECONDS, nx=True)
        await pipe.incr(k24h)
        await pipe.expire(k24h, _24H_SECONDS, nx=True)
        # Record last send timestamp for recency scoring
        await pipe.set(key_last_send(user_id, event_type), datetime.utcnow().timestamp(), ex=_24H_SECONDS)
        await pipe.execute()
    except Exception as e:
        log.warning("dispatcher.counter_update_failed", error=str(e))


async def _persist_event(
    db: AsyncSession,
    event_id: str,
    event: NotificationEventIn,
    fingerprint: str,
    decision: DecisionEnum,
    score: Optional[float],
    scheduled_at: Optional[datetime],
    reason_chain: list[ReasonStep],
    ai_result: ScoringResult,
    rule_matched: Optional[str],
):
    """Write event record to Postgres."""
    try:
        record = NotificationEvent(
            id=event_id,
            user_id=event.user_id,
            event_type=event.event_type,
            title=event.title,
            message=event.message,
            source=event.source,
            channel=event.channel.value,
            priority_hint=event.priority_hint.value if event.priority_hint else None,
            dedupe_key=event.dedupe_key,
            computed_fingerprint=fingerprint,
            expires_at=event.expires_at,
            event_timestamp=event.timestamp or datetime.utcnow(),
            metadata_=event.metadata or {},
            decision=decision.value,
            score=score,
            scheduled_at=scheduled_at,
            decision_reason=[s.model_dump() for s in reason_chain],
            ai_used=ai_result.ai_used,
            fallback_used=ai_result.fallback_used,
            rule_matched=rule_matched,
            processed_at=datetime.utcnow(),
        )
        db.add(record)
    except Exception as e:
        log.error("dispatcher.persist_event_failed", error=str(e))


async def _write_audit_log(
    db: AsyncSession,
    event_id: str,
    event: NotificationEventIn,
    decision: DecisionEnum,
    score: Optional[float],
    ai_result: ScoringResult,
    rule_matched: Optional[str],
    reason_chain: list[ReasonStep],
):
    try:
        entry = AuditLog(
            event_id=event_id,
            user_id=event.user_id,
            event_type=event.event_type,
            decision=decision.value,
            score=score,
            ai_used=ai_result.ai_used,
            fallback_used=ai_result.fallback_used,
            rule_matched=rule_matched,
            reason_chain=[s.model_dump() for s in reason_chain],
            raw_event=event.model_dump(mode="json"),
        )
        db.add(entry)
    except Exception as e:
        log.error("dispatcher.audit_log_failed", error=str(e))


async def _ensure_digest_batch(
    db: AsyncSession,
    event_id: str,
    event: NotificationEventIn,
    scheduled_at: datetime,
):
    """Add event to an existing digest batch or create a new one."""
    try:
        # Find an open batch for this user+channel within the batch window
        window_start = scheduled_at
        window_end = scheduled_at.replace(
            minute=((scheduled_at.minute // 30) + 1) * 30 % 60,
            second=0, microsecond=0,
        )
        result = await db.execute(
            select(DigestBatch).where(
                DigestBatch.user_id == event.user_id,
                DigestBatch.channel == event.channel.value,
                DigestBatch.status == "pending",
                DigestBatch.scheduled_at >= window_start,
            ).limit(1)
        )
        batch = result.scalar_one_or_none()

        if batch:
            ids = batch.event_ids or []
            ids.append(event_id)
            batch.event_ids = ids
        else:
            batch = DigestBatch(
                user_id=event.user_id,
                channel=event.channel.value,
                event_ids=[event_id],
                scheduled_at=scheduled_at,
            )
            db.add(batch)
    except Exception as e:
        log.warning("dispatcher.digest_batch_failed", error=str(e))


async def dispatch(
    event_id: str,
    event: NotificationEventIn,
    fingerprint: str,
    decision: DecisionEnum,
    score: Optional[float],
    scheduled_at: Optional[datetime],
    reason_chain: list[ReasonStep],
    ai_result: ScoringResult,
    rule_matched: Optional[str],
    ctx: UserContext,
    db: AsyncSession,
) -> DecisionResult:
    """
    Route the decision and persist all records.
    Returns the final DecisionResult.
    """
    now = datetime.utcnow()

    # ── Persist event + audit log ─────────────────────────────────
    await _persist_event(
        db, event_id, event, fingerprint, decision, score,
        scheduled_at, reason_chain, ai_result, rule_matched
    )
    await _write_audit_log(
        db, event_id, event, decision, score,
        ai_result, rule_matched, reason_chain
    )

    if decision == DecisionEnum.now:
        # ── NOW: Publish to send queue + update counters ──────────
        await publish(
            settings.kafka_topic_send_now,
            {
                "event_id": event_id,
                "user_id": event.user_id,
                "event_type": event.event_type,
                "title": event.title,
                "message": event.message,
                "channel": event.channel.value,
                "source": event.source,
                "metadata": event.metadata,
                "dispatched_at": now.isoformat(),
            },
            key=event.user_id,
        )
        await _increment_fatigue_counters(event.user_id, event.event_type)
        await register_cooldown(event, ttl_seconds=settings.default_cooldown_seconds)
        log.info("dispatcher.sent_now", event_id=event_id, user_id=event.user_id, event_type=event.event_type)

    elif decision == DecisionEnum.later:
        # ── LATER: Publish to defer queue + digest batch ──────────
        await publish(
            settings.kafka_topic_defer,
            {
                "event_id": event_id,
                "user_id": event.user_id,
                "scheduled_at": scheduled_at.isoformat() if scheduled_at else None,
                "channel": event.channel.value,
            },
            key=event.user_id,
        )
        if scheduled_at:
            await _ensure_digest_batch(db, event_id, event, scheduled_at)
        log.info("dispatcher.deferred", event_id=event_id, scheduled_at=scheduled_at)

    else:
        # ── NEVER: Audit only — no delivery ──────────────────────
        log.info("dispatcher.suppressed", event_id=event_id, reason=rule_matched or "score_below_threshold")

    return DecisionResult(
        event_id=event_id,
        user_id=event.user_id,
        decision=decision,
        score=score,
        scheduled_at=scheduled_at,
        reason_chain=reason_chain,
        ai_used=ai_result.ai_used,
        fallback_used=ai_result.fallback_used,
        processed_at=now,
    )
