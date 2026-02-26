"""
AI Scorer — uses Groq (llama-3.1-8b-instant) for low-latency scoring.
Falls back to heuristic scorer automatically via circuit breaker.
"""
import json
import asyncio
import uuid
from dataclasses import dataclass
from typing import Optional

import structlog
from circuitbreaker import circuit, CircuitBreakerError
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.models.schemas import NotificationEventIn, ReasonStep
from app.services.context_enricher import UserContext

log = structlog.get_logger()
settings = get_settings()

# ── Groq Client (lazy init) ───────────────────────────────────────────────────
_groq_client = None


def _get_groq():
    global _groq_client
    if _groq_client is None and settings.groq_api_key:
        try:
            from groq import Groq
            _groq_client = Groq(api_key=settings.groq_api_key)
        except ImportError:
            log.warning("groq.not_installed")
    return _groq_client


@dataclass
class ScoringResult:
    score: float
    decision_hint: str       # "now" | "later" | "never"
    urgency: float
    engagement: float
    fatigue_penalty: float
    recency_bonus: float
    reasoning: str
    ai_used: bool = True
    fallback_used: bool = False


def _build_prompt(event: NotificationEventIn, ctx: UserContext) -> str:
    return f"""You are a notification prioritization engine. Analyze this notification and return ONLY valid JSON — no explanation, no markdown.

NOTIFICATION EVENT:
- event_type: {event.event_type}
- title: {event.title}
- message: {event.message[:300]}
- source: {event.source}
- channel: {event.channel.value}
- priority_hint: {event.priority_hint.value if event.priority_hint else "none"}

USER CONTEXT:
- notifications_sent_last_1h: {ctx.notifications_last_1h} (cap: {ctx.hourly_cap})
- notifications_sent_last_24h: {ctx.notifications_last_24h} (cap: {ctx.daily_cap})
- seconds_since_last_same_type: {ctx.seconds_since_last_same_type or "never_sent"}
- dnd_active: {ctx.dnd_active}
- current_local_hour: {ctx.current_local_hour}
- user_segment: {ctx.segment}
- engagement_at_current_hour: {ctx.engagement_score_for_current_hour:.2f}
- opted_out_topics: {ctx.opted_out_topics}

SCORING FORMULA: score = (0.35 * urgency) + (0.25 * engagement) - (0.25 * fatigue_penalty) + (0.15 * recency_bonus)

Return this exact JSON structure:
{{
  "score": <float 0.0-1.0>,
  "decision": "<now|later|never>",
  "urgency": <float 0.0-1.0>,
  "engagement": <float 0.0-1.0>,
  "fatigue_penalty": <float 0.0-1.0>,
  "recency_bonus": <float 0.0-1.0>,
  "reasoning": "<one sentence explanation>"
}}"""


@circuit(
    failure_threshold=3,
    recovery_timeout=30,
    expected_exception=Exception,
    name="groq_scorer",
)
async def _call_groq(prompt: str) -> dict:
    """Call Groq API with circuit breaker protection."""
    client = _get_groq()
    if not client:
        raise RuntimeError("Groq client not available — API key missing")

    # Run sync Groq call in thread pool to not block event loop
    loop = asyncio.get_event_loop()
    response = await asyncio.wait_for(
        loop.run_in_executor(
            None,
            lambda: client.chat.completions.create(
                model=settings.groq_model,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                temperature=0.1,
                max_tokens=256,
            ),
        ),
        timeout=settings.groq_timeout_seconds,
    )

    raw = response.choices[0].message.content
    return json.loads(raw)


async def _save_ai_log(
    db: AsyncSession,
    event_id: str,
    event: NotificationEventIn,
    prompt: str,
    result: ScoringResult,
    raw_response: Optional[dict] = None,
    fallback_reason: Optional[str] = None,
):
    """Persist the full AI prompt + response to the database."""
    try:
        from app.models.tables import AIInteractionLog
        from datetime import datetime
        entry = AIInteractionLog(
            id=str(uuid.uuid4()),
            event_id=event_id,
            user_id=event.user_id,
            event_type=event.event_type,
            prompt=prompt,
            response=raw_response,
            ai_used=result.ai_used,
            fallback_reason=fallback_reason,
            score=result.score,
            decision=result.decision_hint,
            urgency=result.urgency,
            engagement=result.engagement,
            fatigue_penalty=result.fatigue_penalty,
            recency_bonus=result.recency_bonus,
            reasoning=result.reasoning,
        )
        db.add(entry)
        await db.flush()
        log.info("ai_scorer.log_saved", event_id=event_id, decision=result.decision_hint, score=result.score)
    except Exception as e:
        log.warning("ai_scorer.log_save_failed", error=str(e))


async def score_with_ai(
    event: NotificationEventIn,
    ctx: UserContext,
    db: Optional[AsyncSession] = None,
    event_id: Optional[str] = None,
) -> ScoringResult:
    """
    Score with Groq. Falls back to heuristic if Groq is unavailable.
    Saves full prompt + response to DB if a session is provided.
    """
    prompt = _build_prompt(event, ctx)

    log.info("ai_scorer.prompt_sent", user_id=event.user_id, event_type=event.event_type)

    try:
        data = await _call_groq(prompt)

        log.info("ai_scorer.response_received", user_id=event.user_id, response=data)

        result = ScoringResult(
            score=float(data.get("score", 0.5)),
            decision_hint=str(data.get("decision", "later")),
            urgency=float(data.get("urgency", 0.5)),
            engagement=float(data.get("engagement", 0.5)),
            fatigue_penalty=float(data.get("fatigue_penalty", 0.0)),
            recency_bonus=float(data.get("recency_bonus", 0.5)),
            reasoning=str(data.get("reasoning", "AI scored this event")),
            ai_used=True,
            fallback_used=False,
        )
        log.info("ai_scorer.groq_success", score=result.score, decision=result.decision_hint)

        if db and event_id:
            await _save_ai_log(db, event_id, event, prompt, result, raw_response=data)

        return result

    except CircuitBreakerError:
        log.warning("ai_scorer.circuit_open_using_heuristic")
        result = _heuristic_score(event, ctx, fallback_reason="circuit_breaker_open")
        if db and event_id:
            await _save_ai_log(db, event_id, event, prompt, result, fallback_reason="circuit_breaker_open")
        return result

    except asyncio.TimeoutError:
        log.warning("ai_scorer.groq_timeout", timeout=settings.groq_timeout_seconds)
        result = _heuristic_score(event, ctx, fallback_reason="groq_timeout")
        if db and event_id:
            await _save_ai_log(db, event_id, event, prompt, result, fallback_reason="groq_timeout")
        return result

    except Exception as e:
        log.error("ai_scorer.groq_error", error=str(e))
        result = _heuristic_score(event, ctx, fallback_reason=f"groq_error:{type(e).__name__}")
        if db and event_id:
            await _save_ai_log(db, event_id, event, prompt, result, fallback_reason=f"groq_error:{type(e).__name__}")
        return result


# ── Heuristic Fallback Scorer ─────────────────────────────────────────────────

# Base urgency by event_type keyword
_URGENCY_MAP = {
    "critical": 1.0,
    "security": 1.0,
    "payment_failed": 1.0,
    "payment_declined": 1.0,
    "2fa": 1.0,
    "otp": 1.0,
    "password": 0.9,
    "account": 0.8,
    "message": 0.7,
    "reminder": 0.7,
    "alert": 0.8,
    "update": 0.5,
    "system": 0.5,
    "promo": 0.2,
    "promotion": 0.2,
    "marketing": 0.15,
    "offer": 0.2,
    "discount": 0.2,
    "newsletter": 0.1,
}

_PRIORITY_HINT_URGENCY = {
    "critical": 1.0,
    "high": 0.8,
    "medium": 0.5,
    "low": 0.2,
}


def _event_type_urgency(event_type: str) -> float:
    et = event_type.lower()
    for keyword, urgency in _URGENCY_MAP.items():
        if keyword in et:
            return urgency
    return 0.4  # Unknown = medium


def _heuristic_score(
    event: NotificationEventIn,
    ctx: UserContext,
    fallback_reason: str = "heuristic_primary",
) -> ScoringResult:
    """Pure heuristic scoring — no external dependencies."""
    urgency = _event_type_urgency(event.event_type)

    # Override with priority_hint if present
    if event.priority_hint:
        hint_urgency = _PRIORITY_HINT_URGENCY.get(event.priority_hint.value, 0.4)
        urgency = max(urgency, hint_urgency)

    engagement = ctx.engagement_score_for_current_hour
    fatigue_penalty = ctx.fatigue_ratio_1h
    recency_bonus = ctx.recency_bonus

    score = (
        0.35 * urgency
        + 0.25 * engagement
        - 0.25 * fatigue_penalty
        + 0.15 * recency_bonus
    )
    score = max(0.0, min(1.0, score))

    if score >= settings.ai_score_now_threshold:
        decision = "now"
    elif score >= settings.ai_score_later_threshold:
        decision = "later"
    else:
        decision = "never"

    return ScoringResult(
        score=score,
        decision_hint=decision,
        urgency=urgency,
        engagement=engagement,
        fatigue_penalty=fatigue_penalty,
        recency_bonus=recency_bonus,
        reasoning=f"Heuristic scorer ({fallback_reason}): urgency={urgency:.2f}, fatigue={fatigue_penalty:.2f}",
        ai_used=False,
        fallback_used=True,
    )


def score_reason_step(result: ScoringResult) -> ReasonStep:
    scorer_label = "groq_llm" if result.ai_used else "heuristic_fallback"
    return ReasonStep(
        layer="L4-AIScorer",
        check=scorer_label,
        result=result.decision_hint.upper(),
        detail=(
            f"score={result.score:.3f} | urgency={result.urgency:.2f} | "
            f"engagement={result.engagement:.2f} | fatigue={result.fatigue_penalty:.2f} | "
            f"recency={result.recency_bonus:.2f} | {result.reasoning}"
        ),
    )