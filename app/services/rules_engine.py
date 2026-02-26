"""
Rules Engine — evaluates human-configurable rules from Redis cache.
Rules are stored in Postgres and hot-reloaded into Redis on change.
No code deployment required to update rules.
"""
import json
import asyncio
from datetime import datetime
from typing import Optional, Tuple

import structlog
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.config import get_settings
from app.models.schemas import NotificationEventIn, ReasonStep
from app.models.tables import RuleConfig
from app.utils.redis_client import get_redis, key_rules_cache

log = structlog.get_logger()
settings = get_settings()

# In-memory rule cache (reloaded periodically)
_rules_cache: list[dict] = []
_rules_loaded_at: datetime | None = None
_CACHE_TTL_SECONDS = 30


async def _load_rules_from_db(db: AsyncSession) -> list[dict]:
    result = await db.execute(
        select(RuleConfig)
        .where(RuleConfig.is_active == True)
        .order_by(RuleConfig.priority_order)
    )
    rules = result.scalars().all()
    return [
        {
            "id": r.id,
            "rule_name": r.rule_name,
            "rule_type": r.rule_type,
            "conditions": r.conditions,
            "action_params": r.action_params,
            "priority_order": r.priority_order,
        }
        for r in rules
    ]


async def get_active_rules(db: AsyncSession | None = None) -> list[dict]:
    """Get rules from in-memory cache, refresh if stale."""
    global _rules_cache, _rules_loaded_at

    now = datetime.utcnow()
    cache_age = (now - _rules_loaded_at).total_seconds() if _rules_loaded_at else 999

    if cache_age > _CACHE_TTL_SECONDS and db:
        _rules_cache = await _load_rules_from_db(db)
        _rules_loaded_at = now
        log.info("rules_engine.cache_refreshed", count=len(_rules_cache))

    return _rules_cache


async def invalidate_rules_cache():
    """Called after rule CRUD operations."""
    global _rules_loaded_at
    _rules_loaded_at = None
    log.info("rules_engine.cache_invalidated")


def _matches_conditions(event: NotificationEventIn, conditions: dict) -> bool:
    """
    Evaluate conditions against event fields.
    Condition keys map to event fields; values can be:
      - A list (event field must be IN the list)
      - A single value (exact match)
      - A dict with operators: {"gte": 5}, {"contains": "fail"}
    """
    event_dict = {
        "event_type": event.event_type,
        "source": event.source,
        "channel": event.channel.value,
        "priority_hint": event.priority_hint.value if event.priority_hint else None,
        "user_id": event.user_id,
    }
    # Merge metadata into checkable fields
    if event.metadata:
        event_dict.update({f"meta.{k}": v for k, v in event.metadata.items()})

    for cond_key, cond_val in conditions.items():
        event_val = event_dict.get(cond_key)

        if isinstance(cond_val, list):
            if event_val not in cond_val:
                return False
        elif isinstance(cond_val, dict):
            # Operator-based conditions
            for op, operand in cond_val.items():
                if op == "gte" and not (event_val is not None and event_val >= operand):
                    return False
                elif op == "lte" and not (event_val is not None and event_val <= operand):
                    return False
                elif op == "contains" and not (
                    event_val and operand.lower() in str(event_val).lower()
                ):
                    return False
                elif op == "not_in" and event_val in operand:
                    return False
        else:
            if event_val != cond_val:
                return False

    return True


def _is_quiet_hours(action_params: dict) -> bool:
    """Check if current UTC hour falls in defined quiet hours."""
    now_hour = datetime.utcnow().hour
    start = action_params.get("start_hour", 22)
    end = action_params.get("end_hour", 8)
    if start > end:  # Overnight (e.g. 22 → 8)
        return now_hour >= start or now_hour < end
    return start <= now_hour < end


async def evaluate_rules(
    event: NotificationEventIn,
    db: AsyncSession | None = None,
) -> Tuple[Optional[str], Optional[str], list[ReasonStep]]:
    """
    Evaluate all active rules against event.
    Returns: (decision | None, rule_name | None, reason_steps)
    decision is None if no rule fires a hard outcome.
    """
    rules = await get_active_rules(db)
    steps: list[ReasonStep] = []

    for rule in rules:
        rule_type = rule["rule_type"]
        conditions = rule["conditions"]
        action_params = rule.get("action_params", {})
        rule_name = rule["rule_name"]

        if not _matches_conditions(event, conditions):
            continue

        # ── Force NOW ──────────────────────────────────────────────
        if rule_type == "force_now":
            step = ReasonStep(
                layer="L2-Rules",
                check=f"rule:{rule_name}",
                result="FORCE_NOW",
                detail=f"Rule '{rule_name}' forces immediate delivery",
            )
            steps.append(step)
            return "now", rule_name, steps

        # ── Force NEVER ────────────────────────────────────────────
        elif rule_type == "force_never":
            step = ReasonStep(
                layer="L2-Rules",
                check=f"rule:{rule_name}",
                result="FORCE_NEVER",
                detail=f"Rule '{rule_name}' suppresses this notification",
            )
            steps.append(step)
            return "never", rule_name, steps

        # ── Quiet Hours ────────────────────────────────────────────
        elif rule_type == "quiet_hours":
            if _is_quiet_hours(action_params):
                step = ReasonStep(
                    layer="L2-Rules",
                    check=f"rule:{rule_name}",
                    result="DEFER",
                    detail=f"Quiet hours active ({action_params.get('start_hour')}–{action_params.get('end_hour')} UTC)",
                )
                steps.append(step)
                return "later", rule_name, steps

        # ── Channel Override ───────────────────────────────────────
        elif rule_type == "channel_override":
            allowed_channels = action_params.get("allowed_channels", [])
            if event.channel.value not in allowed_channels:
                step = ReasonStep(
                    layer="L2-Rules",
                    check=f"rule:{rule_name}",
                    result="FORCE_NEVER",
                    detail=f"Channel '{event.channel.value}' not in allowed: {allowed_channels}",
                )
                steps.append(step)
                return "never", rule_name, steps

        # Log matching rule that didn't force a decision (e.g. cooldown hint)
        steps.append(ReasonStep(
            layer="L2-Rules",
            check=f"rule:{rule_name}",
            result="MATCHED_NO_FORCE",
            detail=f"Rule '{rule_name}' matched but did not force decision",
        ))

    steps.append(ReasonStep(
        layer="L2-Rules",
        check="rules_evaluation",
        result="NO_MATCH",
        detail=f"Evaluated {len(rules)} rules — no hard outcome",
    ))
    return None, None, steps
