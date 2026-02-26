"""
Scheduler — background worker that processes deferred notifications.
Runs on a configurable poll interval, picks up due DigestBatches,
and moves them to the send_now_queue.
"""
import asyncio
from datetime import datetime

import structlog
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

from app.config import get_settings
from app.models.tables import Base, DigestBatch, NotificationEvent
from app.utils.kafka_client import publish

log = structlog.get_logger()
settings = get_settings()


async def process_due_batches(db: AsyncSession):
    """Find and dispatch all digest batches that are due."""
    now = datetime.utcnow()

    result = await db.execute(
        select(DigestBatch).where(
            DigestBatch.status == "pending",
            DigestBatch.scheduled_at <= now,
        ).limit(100)
    )
    batches = result.scalars().all()

    if not batches:
        return

    log.info("scheduler.processing_batches", count=len(batches))

    for batch in batches:
        try:
            # Fetch event details for all events in batch
            event_result = await db.execute(
                select(NotificationEvent).where(
                    NotificationEvent.id.in_(batch.event_ids or [])
                )
            )
            events = event_result.scalars().all()

            # Filter out expired events
            valid_events = [
                e for e in events
                if not e.expires_at or e.expires_at > now
            ]

            if not valid_events:
                log.info("scheduler.batch_all_expired", batch_id=batch.id)
                batch.status = "cancelled"
                batch.sent_at = now
                continue

            # Publish digest to send_now_queue
            if len(valid_events) == 1:
                # Single event — send directly
                e = valid_events[0]
                await publish(
                    settings.kafka_topic_send_now,
                    {
                        "event_id": e.id,
                        "user_id": e.user_id,
                        "event_type": e.event_type,
                        "title": e.title,
                        "message": e.message,
                        "channel": e.channel,
                        "source": e.source,
                        "metadata": e.metadata_,
                        "dispatched_at": now.isoformat(),
                        "scheduled_send": True,
                    },
                    key=e.user_id,
                )
            else:
                # Multiple events — send as digest
                digest_items = [
                    {
                        "event_id": e.id,
                        "event_type": e.event_type,
                        "title": e.title,
                        "message": e.message,
                        "source": e.source,
                    }
                    for e in sorted(valid_events, key=lambda x: x.metadata_.get("priority_order", 5))
                ]
                await publish(
                    settings.kafka_topic_send_now,
                    {
                        "batch_id": batch.id,
                        "user_id": batch.user_id,
                        "channel": batch.channel,
                        "type": "digest",
                        "items": digest_items,
                        "item_count": len(digest_items),
                        "dispatched_at": now.isoformat(),
                    },
                    key=batch.user_id,
                )

            batch.status = "sent"
            batch.sent_at = now
            log.info(
                "scheduler.batch_sent",
                batch_id=batch.id,
                user_id=batch.user_id,
                event_count=len(valid_events),
            )

        except Exception as e:
            log.error("scheduler.batch_error", batch_id=batch.id, error=str(e))

    await db.commit()


async def run_scheduler():
    """Main scheduler loop."""
    log.info("scheduler.starting", poll_interval=settings.scheduler_poll_interval_seconds)

    engine = create_async_engine(settings.database_url, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    SessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    while True:
        try:
            async with SessionLocal() as db:
                await process_due_batches(db)
        except Exception as e:
            log.error("scheduler.loop_error", error=str(e))

        await asyncio.sleep(settings.scheduler_poll_interval_seconds)


if __name__ == "__main__":
    import structlog
    structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(20))
    asyncio.run(run_scheduler())
