"""
Context Enricher — gathers all per-user context needed for scoring.
Runs dedup, profile fetch, and counter reads in parallel.
Fails safely: returns defaults if any source is unavailable.
"""
import asyncio
import json
import pytz
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional

import structlog
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.config import get_settings
from app.models.schemas import NotificationEventIn
from app.models.tables import UserProfile
from app.utils.redis_client import (
    get_redis,
    key_count_1h,
    key_count_24h,
    key_last_send,
    key_user_profile_cache,
)

log = structlog.get_logger()
settings = get_settings()


@dataclass
class UserContext:
    user_id: str
    # Fatigue counters
    notifications_last_1h: int = 0
    notifications_last_24h: int = 0
    # Caps
    hourly_cap: int = 5
    daily_cap: int = 20
    # Time context
    dnd_active: bool = False
    dnd_start_hour: int = 22
    dnd_end_hour: int = 8
    timezone: str = "UTC"
    current_local_hour: int = 12
    # Preferences
    opted_out_topics: list = field(default_factory=list)
    channel_preferences: dict = field(default_factory=dict)
    segment: str = "standard"
    # Recency
    seconds_since_last_same_type: Optional[float] = None
    # Engagement
    engagement_heatmap: list = field(default_factory=lambda: [1.0] * 24)
    # Flags
    profile_found: bool = False

    @property
    def hourly_cap_hit(self) -> bool:
        return self.notifications_last_1h >= self.hourly_cap

    @property
    def daily_cap_hit(self) -> bool:
        return self.notifications_last_24h >= self.daily_cap

    @property
    def fatigue_ratio_1h(self) -> float:
        if self.hourly_cap == 0:
            return 1.0
        return min(self.notifications_last_1h / self.hourly_cap, 1.0)

    @property
    def engagement_score_for_current_hour(self) -> float:
        if len(self.engagement_heatmap) == 24:
            return self.engagement_heatmap[self.current_local_hour]
        return 0.5

    @property
    def recency_bonus(self) -> float:
        """0.0 (just sent) → 1.0 (long ago / never)"""
        if self.seconds_since_last_same_type is None:
            return 1.0
        cooldown = settings.default_cooldown_seconds
        return min(self.seconds_since_last_same_type / cooldown, 1.0)


def _is_dnd_active(start: int, end: int, current_hour: int) -> bool:
    if start > end:  # Overnight
        return current_hour >= start or current_hour < end
    return start <= current_hour < end


async def _fetch_redis_counters(user_id: str) -> tuple[int, int, dict]:
    """Return (count_1h, count_24h, {event_type: last_timestamp})."""
    r = await get_redis()
    try:
        count_1h = int(await r.get(key_count_1h(user_id)) or 0)
        count_24h = int(await r.get(key_count_24h(user_id)) or 0)
        return count_1h, count_24h, {}
    except Exception as e:
        log.warning("context_enricher.redis_counter_failed", error=str(e))
        return 0, 0, {}


async def _fetch_last_send(user_id: str, event_type: str) -> Optional[float]:
    """Return seconds since last send of this event_type, or None."""
    r = await get_redis()
    try:
        key = key_last_send(user_id, event_type)
        ts_str = await r.get(key)
        if ts_str:
            last_ts = float(ts_str)
            now_ts = datetime.utcnow().timestamp()
            return max(0.0, now_ts - last_ts)
        return None
    except Exception:
        return None


async def _fetch_user_profile(user_id: str, db: AsyncSession | None) -> Optional[dict]:
    """Try Redis cache first, then Postgres."""
    r = await get_redis()
    cache_key = key_user_profile_cache(user_id)

    # Try Redis
    try:
        cached = await r.get(cache_key)
        if cached:
            return json.loads(cached)
    except Exception:
        pass

    # Try Postgres
    if db:
        try:
            result = await db.execute(
                select(UserProfile).where(UserProfile.user_id == user_id)
            )
            profile = result.scalar_one_or_none()
            if profile:
                data = {
                    "timezone": profile.timezone,
                    "dnd_start_hour": profile.dnd_start_hour,
                    "dnd_end_hour": profile.dnd_end_hour,
                    "channel_preferences": profile.channel_preferences or {},
                    "opted_out_topics": profile.opted_out_topics or [],
                    "hourly_cap_override": profile.hourly_cap_override,
                    "daily_cap_override": profile.daily_cap_override,
                    "segment": profile.segment,
                    "engagement_heatmap": profile.engagement_heatmap or [1.0] * 24,
                }
                # Cache in Redis for 5 minutes
                try:
                    await r.set(cache_key, json.dumps(data), ex=300)
                except Exception:
                    pass
                return data
        except Exception as e:
            log.warning("context_enricher.db_profile_failed", error=str(e))

    return None


async def enrich_context(
    event: NotificationEventIn,
    db: AsyncSession | None = None,
) -> UserContext:
    """Build full user context for scoring. Never raises — returns safe defaults."""
    ctx = UserContext(user_id=event.user_id)

    # Run all fetches in parallel
    counters_task = asyncio.create_task(_fetch_redis_counters(event.user_id))
    last_send_task = asyncio.create_task(_fetch_last_send(event.user_id, event.event_type))
    profile_task = asyncio.create_task(_fetch_user_profile(event.user_id, db))

    try:
        (count_1h, count_24h, _), last_send_seconds, profile = await asyncio.gather(
            counters_task, last_send_task, profile_task,
            return_exceptions=True,
        )
    except Exception as e:
        log.error("context_enricher.gather_failed", error=str(e))
        return ctx  # Return safe defaults

    # Apply counters
    if isinstance(count_1h, int):
        ctx.notifications_last_1h = count_1h
    if isinstance(count_24h, int):
        ctx.notifications_last_24h = count_24h
    if isinstance(last_send_seconds, float):
        ctx.seconds_since_last_same_type = last_send_seconds

    # Apply profile
    if profile and isinstance(profile, dict):
        ctx.profile_found = True
        ctx.timezone = profile.get("timezone", "UTC")
        ctx.dnd_start_hour = profile.get("dnd_start_hour", 22)
        ctx.dnd_end_hour = profile.get("dnd_end_hour", 8)
        ctx.channel_preferences = profile.get("channel_preferences", {})
        ctx.opted_out_topics = profile.get("opted_out_topics", [])
        ctx.segment = profile.get("segment", "standard")
        ctx.engagement_heatmap = profile.get("engagement_heatmap", [1.0] * 24)

        if profile.get("hourly_cap_override"):
            ctx.hourly_cap = profile["hourly_cap_override"]
        if profile.get("daily_cap_override"):
            ctx.daily_cap = profile["daily_cap_override"]

    # Determine local hour in user's timezone
    try:
        tz = pytz.timezone(ctx.timezone)
        local_now = datetime.now(tz)
        ctx.current_local_hour = local_now.hour
    except Exception:
        ctx.current_local_hour = datetime.utcnow().hour

    ctx.dnd_active = _is_dnd_active(ctx.dnd_start_hour, ctx.dnd_end_hour, ctx.current_local_hour)

    log.debug(
        "context_enricher.done",
        user_id=event.user_id,
        count_1h=ctx.notifications_last_1h,
        dnd_active=ctx.dnd_active,
        profile_found=ctx.profile_found,
    )
    return ctx
