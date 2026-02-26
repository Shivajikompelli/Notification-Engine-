"""
Decision Arbiter — merges rule results, AI scores, and context
into the final Now / Later / Never decision with full reason chain.
"""
from datetime import datetime, timedelta
from typing import Optional, Tuple

import structlog

from app.config import get_settings
from app.models.schemas import NotificationEventIn, ReasonStep, DecisionEnum
from app.services.context_enricher import UserContext
from app.services.ai_scorer import ScoringResult

log = structlog.get_logger()
settings = get_settings()


def _compute_optimal_send_time(ctx: UserContext, expires_at: Optional[datetime]) -> datetime:
    """Find the best hour to send a deferred notification."""
    now = datetime.utcnow()

    # Find highest engagement hour in the next 24h that is NOT in DND
    heatmap = ctx.engagement_heatmap if len(ctx.engagement_heatmap) == 24 else [1.0] * 24
    best_hour = None
    best_score = -1.0

    for offset in range(1, 25):  # Look up to 24h ahead
        candidate = now + timedelta(hours=offset)
        hour = candidate.hour

        # Skip DND hours
        start, end = ctx.dnd_start_hour, ctx.dnd_end_hour
        if start > end:
            in_dnd = hour >= start or hour < end
        else:
            in_dnd = start <= hour < end
        if in_dnd:
            continue

        score = heatmap[hour]
        if score > best_score:
            best_score = score
            best_hour = candidate

    if best_hour is None:
        best_hour = now + timedelta(hours=1)

    # Never schedule past expires_at
    if expires_at and best_hour > expires_at:
        best_hour = expires_at - timedelta(minutes=5)

    # Round to nearest 15 min for clean scheduling
    minutes = best_hour.minute
    rounded = minutes - (minutes % 15)
    best_hour = best_hour.replace(minute=rounded, second=0, microsecond=0)

    return best_hour


def arbitrate(
    event: NotificationEventIn,
    rule_decision: Optional[str],
    rule_name: Optional[str],
    ai_result: ScoringResult,
    ctx: UserContext,
    rule_steps: list[ReasonStep],
    dedup_steps: list[ReasonStep],
    ai_step: ReasonStep,
) -> Tuple[DecisionEnum, Optional[datetime], list[ReasonStep], Optional[str]]:
    """
    Final decision merge.
    Returns: (decision, scheduled_at, reason_chain, override_note)
    """
    reason_chain = dedup_steps + rule_steps

    # ── Step 1: Hard rule wins ────────────────────────────────────
    if rule_decision == "now":
        reason_chain.append(ReasonStep(
            layer="L5-Arbiter",
            check="rule_override",
            result="NOW",
            detail=f"Hard force-now rule '{rule_name}' wins — immediate delivery",
        ))
        return DecisionEnum.now, None, reason_chain, f"rule:{rule_name}"

    if rule_decision == "never":
        reason_chain.append(ReasonStep(
            layer="L5-Arbiter",
            check="rule_override",
            result="NEVER",
            detail=f"Hard suppress rule '{rule_name}' wins — event suppressed",
        ))
        return DecisionEnum.never, None, reason_chain, f"rule:{rule_name}"

    # ── Step 2: Opted-out topic check ────────────────────────────
    if event.event_type in ctx.opted_out_topics:
        reason_chain.append(ReasonStep(
            layer="L5-Arbiter",
            check="topic_opt_out",
            result="NEVER",
            detail=f"User has opted out of '{event.event_type}' notifications",
        ))
        return DecisionEnum.never, None, reason_chain, "user_opt_out"

    # ── Step 3: AI/heuristic score ────────────────────────────────
    reason_chain.append(ai_step)
    score = ai_result.score
    is_critical = event.priority_hint and event.priority_hint.value == "critical"

    # ── Step 4: Fatigue cap enforcement ──────────────────────────
    if ctx.hourly_cap_hit and not is_critical:
        if score < 0.8:  # Very-high-score events still get through even at cap
            if rule_decision == "later":
                scheduled_at = _compute_optimal_send_time(ctx, event.expires_at)
            else:
                scheduled_at = _compute_optimal_send_time(ctx, event.expires_at)
            reason_chain.append(ReasonStep(
                layer="L5-Arbiter",
                check="hourly_cap",
                result="LATER",
                detail=f"Hourly cap hit ({ctx.notifications_last_1h}/{ctx.hourly_cap}) — deferred to {scheduled_at.isoformat()}",
            ))
            return DecisionEnum.later, scheduled_at, reason_chain, "fatigue_hourly_cap"

    if ctx.daily_cap_hit and not is_critical:
        reason_chain.append(ReasonStep(
            layer="L5-Arbiter",
            check="daily_cap",
            result="NEVER",
            detail=f"Daily cap hit ({ctx.notifications_last_24h}/{ctx.daily_cap}) — suppressed",
        ))
        return DecisionEnum.never, None, reason_chain, "fatigue_daily_cap"

    # ── Step 5: DND enforcement ───────────────────────────────────
    if ctx.dnd_active and not is_critical:
        scheduled_at = _compute_optimal_send_time(ctx, event.expires_at)
        reason_chain.append(ReasonStep(
            layer="L5-Arbiter",
            check="dnd_active",
            result="LATER",
            detail=f"DND active ({ctx.dnd_start_hour}–{ctx.dnd_end_hour}) — deferred to {scheduled_at.isoformat()}",
        ))
        return DecisionEnum.later, scheduled_at, reason_chain, "dnd_active"

    # ── Step 6: Rule deferred ─────────────────────────────────────
    if rule_decision == "later":
        scheduled_at = _compute_optimal_send_time(ctx, event.expires_at)
        reason_chain.append(ReasonStep(
            layer="L5-Arbiter",
            check="rule_defer",
            result="LATER",
            detail=f"Rule '{rule_name}' defers — scheduled for {scheduled_at.isoformat()}",
        ))
        return DecisionEnum.later, scheduled_at, reason_chain, f"rule:{rule_name}"

    # ── Step 7: Score thresholds ──────────────────────────────────
    if score >= settings.ai_score_now_threshold or is_critical:
        reason_chain.append(ReasonStep(
            layer="L5-Arbiter",
            check="score_threshold",
            result="NOW",
            detail=f"Score {score:.3f} >= threshold {settings.ai_score_now_threshold} → send now",
        ))
        return DecisionEnum.now, None, reason_chain, None

    elif score >= settings.ai_score_later_threshold:
        scheduled_at = _compute_optimal_send_time(ctx, event.expires_at)
        reason_chain.append(ReasonStep(
            layer="L5-Arbiter",
            check="score_threshold",
            result="LATER",
            detail=f"Score {score:.3f} in [{settings.ai_score_later_threshold}, {settings.ai_score_now_threshold}) → deferred to {scheduled_at.isoformat()}",
        ))
        return DecisionEnum.later, scheduled_at, reason_chain, None

    else:
        reason_chain.append(ReasonStep(
            layer="L5-Arbiter",
            check="score_threshold",
            result="NEVER",
            detail=f"Score {score:.3f} < threshold {settings.ai_score_later_threshold} → suppressed",
        ))
        return DecisionEnum.never, None, reason_chain, None
