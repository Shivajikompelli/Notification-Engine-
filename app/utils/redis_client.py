import redis.asyncio as aioredis
from app.config import get_settings
import structlog

log = structlog.get_logger()
settings = get_settings()

_redis: aioredis.Redis | None = None


async def get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        _redis = aioredis.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
            retry_on_timeout=True,
            max_connections=50,
        )
    return _redis


async def close_redis():
    global _redis
    if _redis:
        await _redis.close()
        _redis = None


# ── Key Builders ──────────────────────────────────────────────────────────────

def key_exact_dedup(fingerprint: str) -> str:
    return f"dedup:exact:{fingerprint}"

def key_near_dedup_minhash(user_id: str, fingerprint: str) -> str:
    return f"dedup:lsh:{user_id}:{fingerprint}"

def key_count_1h(user_id: str) -> str:
    return f"notif:count:{user_id}:1h"

def key_count_24h(user_id: str) -> str:
    return f"notif:count:{user_id}:24h"

def key_last_send(user_id: str, event_type: str) -> str:
    return f"notif:last:{user_id}:{event_type}"

def key_cooldown(user_id: str, event_type: str) -> str:
    return f"notif:cooldown:{user_id}:{event_type}"

def key_rules_cache() -> str:
    return "rules:active"

def key_rules_invalidate() -> str:
    return "rules:invalidate"

def key_user_profile_cache(user_id: str) -> str:
    return f"user:profile:{user_id}"
