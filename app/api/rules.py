"""
Rules API — CRUD for human-configurable business rules.
Changes take effect within 30 seconds (cache TTL) — no deployment needed.
"""
from datetime import datetime

import structlog
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update

from app.models.database import get_db
from app.models.schemas import RuleIn, RuleOut
from app.models.tables import RuleConfig
from app.services.rules_engine import invalidate_rules_cache

log = structlog.get_logger()
router = APIRouter(prefix="/v1/rules", tags=["Rules"])


@router.get("", response_model=list[RuleOut], summary="List all rules")
async def list_rules(
    active_only: bool = False,
    db: AsyncSession = Depends(get_db),
):
    query = select(RuleConfig).order_by(RuleConfig.priority_order)
    if active_only:
        query = query.where(RuleConfig.is_active == True)
    result = await db.execute(query)
    rules = result.scalars().all()
    return [RuleOut(
        id=r.id,
        rule_name=r.rule_name,
        rule_type=r.rule_type,
        conditions=r.conditions,
        action_params=r.action_params or {},
        priority_order=r.priority_order,
        is_active=r.is_active,
        created_at=r.created_at,
        updated_at=r.updated_at,
    ) for r in rules]


@router.post("", response_model=RuleOut, summary="Create a new rule")
async def create_rule(
    rule: RuleIn,
    db: AsyncSession = Depends(get_db),
):
    # Check for name collision
    existing = await db.execute(
        select(RuleConfig).where(RuleConfig.rule_name == rule.rule_name)
    )
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail=f"Rule '{rule.rule_name}' already exists")

    record = RuleConfig(
        rule_name=rule.rule_name,
        rule_type=rule.rule_type.value,
        conditions=rule.conditions,
        action_params=rule.action_params,
        priority_order=rule.priority_order,
        is_active=rule.is_active,
    )
    db.add(record)
    await db.flush()
    await invalidate_rules_cache()
    log.info("rules.created", rule_name=rule.rule_name, rule_id=record.id)
    return RuleOut(
        id=record.id, rule_name=record.rule_name, rule_type=record.rule_type,
        conditions=record.conditions, action_params=record.action_params or {},
        priority_order=record.priority_order, is_active=record.is_active,
        created_at=record.created_at, updated_at=record.updated_at,
    )


@router.put("/{rule_id}", response_model=RuleOut, summary="Update a rule")
async def update_rule(
    rule_id: str,
    rule: RuleIn,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(RuleConfig).where(RuleConfig.id == rule_id))
    record = result.scalar_one_or_none()
    if not record:
        raise HTTPException(status_code=404, detail="Rule not found")

    record.rule_name = rule.rule_name
    record.rule_type = rule.rule_type.value
    record.conditions = rule.conditions
    record.action_params = rule.action_params
    record.priority_order = rule.priority_order
    record.is_active = rule.is_active
    record.updated_at = datetime.utcnow()

    await db.flush()
    await invalidate_rules_cache()
    log.info("rules.updated", rule_id=rule_id, rule_name=rule.rule_name)
    return RuleOut(
        id=record.id, rule_name=record.rule_name, rule_type=record.rule_type,
        conditions=record.conditions, action_params=record.action_params or {},
        priority_order=record.priority_order, is_active=record.is_active,
        created_at=record.created_at, updated_at=record.updated_at,
    )


@router.patch("/{rule_id}/toggle", summary="Toggle rule active/inactive")
async def toggle_rule(
    rule_id: str,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(RuleConfig).where(RuleConfig.id == rule_id))
    record = result.scalar_one_or_none()
    if not record:
        raise HTTPException(status_code=404, detail="Rule not found")

    record.is_active = not record.is_active
    record.updated_at = datetime.utcnow()
    await db.flush()
    await invalidate_rules_cache()
    return {"rule_id": rule_id, "is_active": record.is_active, "message": "Rule toggled"}


@router.delete("/{rule_id}", summary="Delete a rule")
async def delete_rule(
    rule_id: str,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(RuleConfig).where(RuleConfig.id == rule_id))
    record = result.scalar_one_or_none()
    if not record:
        raise HTTPException(status_code=404, detail="Rule not found")

    await db.delete(record)
    await invalidate_rules_cache()
    log.info("rules.deleted", rule_id=rule_id)
    return {"message": f"Rule {rule_id} deleted"}
