"""
Notifications API — core endpoints for event evaluation and audit retrieval.
"""
import uuid
import asyncio
from datetime import datetime
from typing import List, Optional

import structlog
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.models.database import get_db
from app.models.schemas import (
    NotificationEventIn, BatchNotificationEventIn,
    DecisionResult, BatchDecisionResult, AuditEntry,
)
from app.models.tables import NotificationEvent, AuditLog, AIInteractionLog
from app.services.pipeline import evaluate_notification

log = structlog.get_logger()
router = APIRouter(prefix="/v1/notifications", tags=["Notifications"])


@router.post(
    "/evaluate",
    response_model=DecisionResult,
    summary="Evaluate a single notification event",
    description="Runs the full Now/Later/Never decision pipeline and returns a structured decision with reason chain.",
)
async def evaluate_single(
    event: NotificationEventIn,
    db: AsyncSession = Depends(get_db),
):
    event_id = str(uuid.uuid4())
    try:
        result = await evaluate_notification(event, db, event_id=event_id)
        return result
    except Exception as e:
        log.error("api.evaluate_failed", event_id=event_id, error=str(e))
        raise HTTPException(status_code=500, detail=f"Evaluation failed: {str(e)}")


@router.post(
    "/batch-evaluate",
    response_model=BatchDecisionResult,
    summary="Evaluate up to 500 notification events",
    description="Evaluates all events concurrently. Returns decisions in same order as input.",
)
async def evaluate_batch(
    payload: BatchNotificationEventIn,
    db: AsyncSession = Depends(get_db),
):
    batch_id = str(uuid.uuid4())
    event_ids = [str(uuid.uuid4()) for _ in payload.events]

    # Evaluate all concurrently (bounded concurrency to avoid overwhelming DB)
    semaphore = asyncio.Semaphore(20)

    async def eval_one(event: NotificationEventIn, eid: str) -> DecisionResult:
        async with semaphore:
            try:
                return await evaluate_notification(event, db, event_id=eid)
            except Exception as e:
                log.error("api.batch_eval_item_failed", event_id=eid, error=str(e))
                from app.models.schemas import ReasonStep, DecisionEnum
                return DecisionResult(
                    event_id=eid,
                    user_id=event.user_id,
                    decision=DecisionEnum.later,  # Fail safe
                    reason_chain=[ReasonStep(
                        layer="L0-Error", check="pipeline_error",
                        result="LATER", detail=f"Pipeline error: {str(e)} — deferred as safe default"
                    )],
                    ai_used=False,
                    fallback_used=True,
                    processed_at=datetime.utcnow(),
                )

    tasks = [eval_one(event, eid) for event, eid in zip(payload.events, event_ids)]
    results = await asyncio.gather(*tasks)

    return BatchDecisionResult(
        batch_id=batch_id,
        total=len(results),
        results=list(results),
        processed_at=datetime.utcnow(),
    )


@router.get(
    "/audit/{event_id}",
    response_model=AuditEntry,
    summary="Get full audit trail for an event",
    description="Returns the complete decision chain, AI features, and raw event for debugging and compliance.",
)
async def get_audit(
    event_id: str,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(AuditLog).where(AuditLog.event_id == event_id).limit(1)
    )
    entry = result.scalar_one_or_none()
    if not entry:
        raise HTTPException(status_code=404, detail=f"Audit entry not found for event_id={event_id}")

    from app.models.schemas import ReasonStep
    return AuditEntry(
        event_id=entry.event_id,
        user_id=entry.user_id,
        event_type=entry.event_type,
        decision=entry.decision,
        score=entry.score,
        ai_used=entry.ai_used,
        fallback_used=entry.fallback_used,
        rule_matched=entry.rule_matched,
        reason_chain=[ReasonStep(**s) for s in (entry.reason_chain or [])],
        raw_event=entry.raw_event or {},
        created_at=entry.created_at,
    )


@router.get(
    "/history/{user_id}",
    summary="Get recent notification decisions for a user",
)
async def get_user_history(
    user_id: str,
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(NotificationEvent)
        .where(NotificationEvent.user_id == user_id)
        .order_by(NotificationEvent.created_at.desc())
        .limit(min(limit, 100))
    )
    events = result.scalars().all()
    return {
        "user_id": user_id,
        "count": len(events),
        "events": [
            {
                "event_id": e.id,
                "event_type": e.event_type,
                "title": e.title,
                "decision": e.decision,
                "score": e.score,
                "ai_used": e.ai_used,
                "created_at": e.created_at.isoformat() if e.created_at else None,
            }
            for e in events
        ],
    }


@router.get(
    "/ai-logs",
    summary="View all AI prompt/response interactions",
    description="Returns stored Groq prompts and responses for inspection and debugging.",
)
async def get_ai_logs(
    user_id: Optional[str] = None,
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
):
    query = select(AIInteractionLog).order_by(AIInteractionLog.created_at.desc())
    if user_id:
        query = query.where(AIInteractionLog.user_id == user_id)
    query = query.limit(min(limit, 100))
    result = await db.execute(query)
    logs = result.scalars().all()
    return {
        "count": len(logs),
        "logs": [
            {
                "id": l.id,
                "event_id": l.event_id,
                "user_id": l.user_id,
                "event_type": l.event_type,
                "ai_used": l.ai_used,
                "fallback_reason": l.fallback_reason,
                "score": l.score,
                "decision": l.decision,
                "urgency": l.urgency,
                "engagement": l.engagement,
                "fatigue_penalty": l.fatigue_penalty,
                "recency_bonus": l.recency_bonus,
                "reasoning": l.reasoning,
                "prompt": l.prompt,
                "response": l.response,
                "created_at": l.created_at.isoformat() if l.created_at else None,
            }
            for l in logs
        ],
    }