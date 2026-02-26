"""
Deduplication Guard — Three-tier strategy:
  Tier 1: Exact match via SHA-256 fingerprint in Redis
  Tier 2: Near-duplicate via MinHash LSH (Jaccard similarity)
  Tier 3: Topic cooldown (same user + event_type within window)
"""
import hashlib
import json
import re
from datetime import datetime
from typing import Tuple

import structlog
from datasketch import MinHash

from app.config import get_settings
from app.models.schemas import NotificationEventIn, ReasonStep
from app.utils.redis_client import (
    get_redis,
    key_exact_dedup,
    key_near_dedup_minhash,
    key_cooldown,
)

log = structlog.get_logger()
settings = get_settings()


def _normalize_text(text: str) -> str:
    """Lowercase, remove punctuation, collapse whitespace."""
    text = text.lower()
    text = re.sub(r"[^\w\s]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _compute_fingerprint(event: NotificationEventIn) -> str:
    """SHA-256 of canonical event identity."""
    key_parts = [
        event.user_id,
        event.event_type,
        event.dedupe_key or _normalize_text(event.title),
        event.source,
    ]
    raw = "|".join(key_parts)
    return hashlib.sha256(raw.encode()).hexdigest()


def _compute_minhash(text: str, num_perm: int = 128) -> MinHash:
    """Generate MinHash signature from 3-grams of text."""
    m = MinHash(num_perm=num_perm)
    normalized = _normalize_text(text)
    # Shingle into character 3-grams
    for i in range(len(normalized) - 2):
        m.update(normalized[i : i + 3].encode("utf-8"))
    return m


def _minhash_to_list(m: MinHash) -> list:
    return m.hashvalues.tolist()


def _jaccard_from_lists(a: list, b: list) -> float:
    """Estimate Jaccard similarity from two MinHash signature lists."""
    matches = sum(1 for x, y in zip(a, b) if x == y)
    return matches / len(a)


async def check_exact_duplicate(fingerprint: str) -> Tuple[bool, ReasonStep]:
    """Return (is_duplicate, reason_step)."""
    r = await get_redis()
    redis_key = key_exact_dedup(fingerprint)
    existing = await r.get(redis_key)

    if existing:
        return True, ReasonStep(
            layer="L1-Dedup",
            check="exact_duplicate",
            result="SUPPRESS",
            detail=f"Fingerprint {fingerprint[:12]}... seen within TTL window",
        )

    # Register fingerprint
    await r.set(redis_key, "1", ex=settings.exact_dedup_ttl_seconds)
    return False, ReasonStep(
        layer="L1-Dedup",
        check="exact_duplicate",
        result="PASS",
        detail="No exact duplicate found",
    )


async def check_near_duplicate(
    event: NotificationEventIn, fingerprint: str
) -> Tuple[bool, ReasonStep]:
    """Return (is_near_duplicate, reason_step)."""
    r = await get_redis()
    text = f"{event.title} {event.message}"
    current_mh = _compute_minhash(text, settings.lsh_num_perm)
    current_sig = _minhash_to_list(current_mh)

    # Scan recent minhash signatures for this user
    pattern = f"dedup:lsh:{event.user_id}:*"
    async for key in r.scan_iter(pattern, count=100):
        stored_raw = await r.get(key)
        if not stored_raw:
            continue
        try:
            stored_sig = json.loads(stored_raw)
            similarity = _jaccard_from_lists(current_sig, stored_sig)
            if similarity >= settings.lsh_jaccard_threshold:
                return True, ReasonStep(
                    layer="L1-Dedup",
                    check="near_duplicate_lsh",
                    result="SUPPRESS",
                    detail=f"Jaccard similarity {similarity:.2f} >= threshold {settings.lsh_jaccard_threshold}",
                )
        except Exception:
            continue

    # Store current signature
    store_key = key_near_dedup_minhash(event.user_id, fingerprint)
    await r.set(
        store_key,
        json.dumps(current_sig),
        ex=settings.near_dedup_ttl_seconds,
    )

    return False, ReasonStep(
        layer="L1-Dedup",
        check="near_duplicate_lsh",
        result="PASS",
        detail="No near-duplicate found above threshold",
    )


async def check_topic_cooldown(event: NotificationEventIn) -> Tuple[bool, ReasonStep]:
    """Check if this event_type is in cooldown for this user."""
    # Critical events bypass cooldown
    if event.priority_hint == "critical":
        return False, ReasonStep(
            layer="L1-Dedup",
            check="topic_cooldown",
            result="BYPASS",
            detail="Critical priority bypasses cooldown",
        )

    r = await get_redis()
    cooldown_key = key_cooldown(event.user_id, event.event_type)
    existing = await r.get(cooldown_key)

    if existing:
        ttl = await r.ttl(cooldown_key)
        return True, ReasonStep(
            layer="L1-Dedup",
            check="topic_cooldown",
            result="DEFER",
            detail=f"Topic {event.event_type} in cooldown — {ttl}s remaining",
        )

    return False, ReasonStep(
        layer="L1-Dedup",
        check="topic_cooldown",
        result="PASS",
        detail="No active cooldown for this topic",
    )


async def register_cooldown(event: NotificationEventIn, ttl_seconds: int):
    """Set a cooldown after a successful send."""
    if event.priority_hint == "critical":
        return  # Critical events don't set cooldowns
    r = await get_redis()
    cooldown_key = key_cooldown(event.user_id, event.event_type)
    await r.set(cooldown_key, "1", ex=ttl_seconds)


async def run_dedup_pipeline(
    event: NotificationEventIn,
) -> Tuple[str | None, str, list[ReasonStep]]:
    """
    Run all dedup checks.
    Returns: (suppress_reason | None, fingerprint, reason_steps)
    suppress_reason is None if event passes all checks.
    """
    fingerprint = _compute_fingerprint(event)
    steps: list[ReasonStep] = []

    # Tier 1: Exact duplicate
    is_dup, step = await check_exact_duplicate(fingerprint)
    steps.append(step)
    if is_dup:
        return "exact_duplicate", fingerprint, steps

    # Tier 2: Near-duplicate (skip for very short messages)
    if len(event.message) > 20:
        is_near_dup, step = await check_near_duplicate(event, fingerprint)
        steps.append(step)
        if is_near_dup:
            return "near_duplicate", fingerprint, steps

    # Tier 3: Topic cooldown
    is_cooling, step = await check_topic_cooldown(event)
    steps.append(step)
    if is_cooling:
        return "topic_cooldown", fingerprint, steps

    return None, fingerprint, steps
