"""
Users API — notification profile, preferences, and fatigue state.
"""
import json
from datetime import datetime

import structlog
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.config import get_settings
from app.models.database import get_db
from app.models.schemas import UserNotificationProfile, UserPreferenceUpdate
from app.models.tables import UserProfile, NotificationEvent
from app.utils.redis_client import get_redis, key_count_1h, key_count_24h, key_user_profile_cache

log = structlog.get_logger()
settings = get_settings()
router = APIRouter(prefix="/v1/users", tags=["Users"])


async def _get_or_create_profile(user_id: str, db: AsyncSession) -> UserProfile:
    result = await db.execute(select(UserProfile).where(UserProfile.user_id == user_id))
    profile = result.scalar_one_or_none()
    if not profile:
        profile = UserProfile(user_id=user_id)
        db.add(profile)
        await db.flush()
    return profile


@router.get(
    "/{user_id}/notification-profile",
    response_model=UserNotificationProfile,
    summary="Get user's notification profile and current fatigue state",
)
async def get_notification_profile(
    user_id: str,
    db: AsyncSession = Depends(get_db),
):
    profile = await _get_or_create_profile(user_id, db)
    r = await get_redis()

    count_1h = int(await r.get(key_count_1h(user_id)) or 0)
    count_24h = int(await r.get(key_count_24h(user_id)) or 0)

    # DND active check
    from app.services.context_enricher import _is_dnd_active
    import pytz
    try:
        tz = pytz.timezone(profile.timezone or "UTC")
        current_hour = datetime.now(tz).hour
    except Exception:
        current_hour = datetime.utcnow().hour
    dnd_active = _is_dnd_active(profile.dnd_start_hour, profile.dnd_end_hour, current_hour)

    # Optimal send hours (top 5 from heatmap, excluding DND)
    heatmap = profile.engagement_heatmap or [1.0] * 24
    scored_hours = sorted(
        [(h, heatmap[h]) for h in range(24)],
        key=lambda x: -x[1]
    )
    # Filter DND hours
    optimal = [h for h, _ in scored_hours if not _is_dnd_active(
        profile.dnd_start_hour, profile.dnd_end_hour, h
    )][:5]

    # Recent decisions
    recent_result = await db.execute(
        select(NotificationEvent)
        .where(NotificationEvent.user_id == user_id)
        .order_by(NotificationEvent.created_at.desc())
        .limit(10)
    )
    recent = recent_result.scalars().all()

    return UserNotificationProfile(
        user_id=user_id,
        notifications_last_1h=count_1h,
        notifications_last_24h=count_24h,
        dnd_active=dnd_active,
        dnd_start_hour=profile.dnd_start_hour,
        dnd_end_hour=profile.dnd_end_hour,
        timezone=profile.timezone or "UTC",
        hourly_cap=profile.hourly_cap_override or settings.default_hourly_cap,
        daily_cap=profile.daily_cap_override or settings.default_daily_cap,
        opted_out_topics=profile.opted_out_topics or [],
        optimal_send_hours=optimal,
        recent_decisions=[
            {
                "event_id": e.id,
                "event_type": e.event_type,
                "decision": e.decision,
                "score": e.score,
                "created_at": e.created_at.isoformat() if e.created_at else None,
            }
            for e in recent
        ],
    )


@router.patch(
    "/{user_id}/preferences",
    summary="Update user notification preferences",
)
async def update_preferences(
    user_id: str,
    prefs: UserPreferenceUpdate,
    db: AsyncSession = Depends(get_db),
):
    profile = await _get_or_create_profile(user_id, db)

    if prefs.dnd_start_hour is not None:
        profile.dnd_start_hour = prefs.dnd_start_hour
    if prefs.dnd_end_hour is not None:
        profile.dnd_end_hour = prefs.dnd_end_hour
    if prefs.timezone is not None:
        profile.timezone = prefs.timezone
    if prefs.channel_preferences is not None:
        profile.channel_preferences = prefs.channel_preferences
    if prefs.opted_out_topics is not None:
        profile.opted_out_topics = prefs.opted_out_topics
    if prefs.hourly_cap_override is not None:
        profile.hourly_cap_override = prefs.hourly_cap_override
    if prefs.daily_cap_override is not None:
        profile.daily_cap_override = prefs.daily_cap_override

    profile.updated_at = datetime.utcnow()

    # Bust the Redis profile cache
    r = await get_redis()
    await r.delete(key_user_profile_cache(user_id))

    return {"message": "Preferences updated", "user_id": user_id}


@router.post(
    "/{user_id}/opt-out/{topic}",
    summary="Opt user out of a notification topic",
)
async def opt_out_topic(
    user_id: str,
    topic: str,
    db: AsyncSession = Depends(get_db),
):
    profile = await _get_or_create_profile(user_id, db)
    topics = list(profile.opted_out_topics or [])
    if topic not in topics:
        topics.append(topic)
        profile.opted_out_topics = topics
        profile.updated_at = datetime.utcnow()

    r = await get_redis()
    await r.delete(key_user_profile_cache(user_id))
    return {"message": f"User {user_id} opted out of '{topic}'", "all_opt_outs": topics}


@router.delete(
    "/{user_id}/opt-out/{topic}",
    summary="Re-subscribe user to a notification topic",
)
async def opt_in_topic(
    user_id: str,
    topic: str,
    db: AsyncSession = Depends(get_db),
):
    profile = await _get_or_create_profile(user_id, db)
    topics = [t for t in (profile.opted_out_topics or []) if t != topic]
    profile.opted_out_topics = topics
    profile.updated_at = datetime.utcnow()

    r = await get_redis()
    await r.delete(key_user_profile_cache(user_id))
    return {"message": f"User {user_id} re-subscribed to '{topic}'", "all_opt_outs": topics}


@router.post(
    "/{user_id}/feedback",
    summary="Record user engagement feedback to improve scoring",
)
async def record_feedback(
    user_id: str,
    event_id: str,
    action: str,  # "opened" | "dismissed" | "muted" | "clicked"
    db: AsyncSession = Depends(get_db),
):
    """
    Feedback loop: user interactions update the engagement heatmap.
    This drives the adaptive personalization of the scoring model.
    """
    allowed_actions = {"opened", "dismissed", "muted", "clicked"}
    if action not in allowed_actions:
        raise HTTPException(status_code=400, detail=f"action must be one of {allowed_actions}")

    profile = await _get_or_create_profile(user_id, db)
    heatmap = list(profile.engagement_heatmap or [1.0] * 24)
    current_hour = datetime.utcnow().hour

    # Update engagement heatmap with exponential moving average
    delta = 0.1  # Learning rate
    if action in ("opened", "clicked"):
        heatmap[current_hour] = min(1.0, heatmap[current_hour] + delta)
    elif action in ("dismissed", "muted"):
        heatmap[current_hour] = max(0.0, heatmap[current_hour] - delta)

    profile.engagement_heatmap = heatmap
    profile.updated_at = datetime.utcnow()

    r = await get_redis()
    await r.delete(key_user_profile_cache(user_id))

    return {"message": "Feedback recorded", "user_id": user_id, "action": action}
