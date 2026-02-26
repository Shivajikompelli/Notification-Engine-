"""
Seed script — populates DB with sample rules, users, and test events.
Run: python -m scripts.seed
"""
import asyncio
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from app.models.tables import Base, RuleConfig, UserProfile
from app.config import get_settings

settings = get_settings()


SAMPLE_RULES = [
    {
        "rule_name": "Force critical payment alerts",
        "rule_type": "force_now",
        "conditions": {"event_type": ["payment_failed", "payment_declined"]},
        "action_params": {},
        "priority_order": 1,
    },
    {
        "rule_name": "Force security alerts",
        "rule_type": "force_now",
        "conditions": {"event_type": ["security_alert", "otp", "2fa", "login_attempt"]},
        "action_params": {},
        "priority_order": 2,
    },
    {
        "rule_name": "Suppress all promos via SMS",
        "rule_type": "channel_override",
        "conditions": {"event_type": ["promo_offer", "newsletter", "marketing"]},
        "action_params": {"allowed_channels": ["push", "email", "in_app"]},
        "priority_order": 10,
    },
    {
        "rule_name": "Quiet hours 22:00-08:00 UTC",
        "rule_type": "quiet_hours",
        "conditions": {},
        "action_params": {"start_hour": 22, "end_hour": 8},
        "priority_order": 20,
    },
    {
        "rule_name": "Suppress free-tier users from SMS",
        "rule_type": "force_never",
        "conditions": {"channel": "sms", "priority_hint": "low"},
        "action_params": {},
        "priority_order": 15,
    },
]

SAMPLE_USERS = [
    {
        "user_id": "user_alice",
        "timezone": "America/New_York",
        "dnd_start_hour": 22,
        "dnd_end_hour": 7,
        "segment": "premium",
        "hourly_cap_override": 8,
        "opted_out_topics": ["newsletter"],
    },
    {
        "user_id": "user_bob",
        "timezone": "Asia/Kolkata",
        "dnd_start_hour": 23,
        "dnd_end_hour": 6,
        "segment": "standard",
        "opted_out_topics": ["promo_offer", "marketing"],
    },
    {
        "user_id": "user_carol",
        "timezone": "Europe/London",
        "dnd_start_hour": 21,
        "dnd_end_hour": 9,
        "segment": "free",
        "daily_cap_override": 5,
    },
]


async def seed():
    engine = create_async_engine(settings.database_url, echo=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with Session() as db:
        # Seed rules
        for r in SAMPLE_RULES:
            from sqlalchemy import select
            existing = await db.execute(
                select(RuleConfig).where(RuleConfig.rule_name == r["rule_name"])
            )
            if not existing.scalar_one_or_none():
                db.add(RuleConfig(**r, is_active=True))
                print(f"[+] Rule: {r['rule_name']}")

        # Seed users
        for u in SAMPLE_USERS:
            from sqlalchemy import select
            existing = await db.execute(
                select(UserProfile).where(UserProfile.user_id == u["user_id"])
            )
            if not existing.scalar_one_or_none():
                db.add(UserProfile(**u))
                print(f"[+] User: {u['user_id']}")

        await db.commit()
    print("\n✅ Seeding complete!")


if __name__ == "__main__":
    asyncio.run(seed())
