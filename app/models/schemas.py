from pydantic import BaseModel, Field, field_validator
from typing import Optional, Any, List
from datetime import datetime
from enum import Enum
import uuid


# ── Enums ────────────────────────────────────────────────────────────────────

class ChannelEnum(str, Enum):
    push = "push"
    email = "email"
    sms = "sms"
    in_app = "in_app"

class PriorityHintEnum(str, Enum):
    critical = "critical"
    high = "high"
    medium = "medium"
    low = "low"

class DecisionEnum(str, Enum):
    now = "now"
    later = "later"
    never = "never"

class RuleTypeEnum(str, Enum):
    force_now = "force_now"
    force_never = "force_never"
    cooldown = "cooldown"
    cap = "cap"
    quiet_hours = "quiet_hours"
    channel_override = "channel_override"


# ── Notification Event ────────────────────────────────────────────────────────

class NotificationEventIn(BaseModel):
    user_id: str = Field(..., min_length=1, max_length=64)
    event_type: str = Field(..., min_length=1, max_length=128)
    title: str = Field(..., min_length=1, max_length=256)
    message: str = Field(..., min_length=1)
    source: str = Field(..., min_length=1, max_length=64)
    channel: ChannelEnum = ChannelEnum.push
    priority_hint: Optional[PriorityHintEnum] = None
    dedupe_key: Optional[str] = Field(None, max_length=256)
    expires_at: Optional[datetime] = None
    timestamp: Optional[datetime] = None
    metadata: Optional[dict[str, Any]] = Field(default_factory=dict)

    @field_validator("expires_at")
    @classmethod
    def expires_must_be_future(cls, v):
        if v and v < datetime.utcnow().replace(tzinfo=v.tzinfo):
            raise ValueError("expires_at must be in the future")
        return v

    model_config = {"json_schema_extra": {
        "example": {
            "user_id": "user_123",
            "event_type": "payment_failed",
            "title": "Payment Failed",
            "message": "Your payment of $49.99 could not be processed.",
            "source": "billing-service",
            "channel": "push",
            "priority_hint": "critical",
            "metadata": {"amount": 49.99, "currency": "USD"}
        }
    }}


class BatchNotificationEventIn(BaseModel):
    events: List[NotificationEventIn] = Field(..., min_length=1, max_length=500)


# ── Decision Result ───────────────────────────────────────────────────────────

class ReasonStep(BaseModel):
    layer: str
    check: str
    result: str
    detail: Optional[str] = None

class DecisionResult(BaseModel):
    event_id: str
    user_id: str
    decision: DecisionEnum
    score: Optional[float] = None
    scheduled_at: Optional[datetime] = None
    reason_chain: List[ReasonStep] = []
    ai_used: bool = False
    fallback_used: bool = False
    processed_at: datetime

class BatchDecisionResult(BaseModel):
    batch_id: str
    total: int
    results: List[DecisionResult]
    processed_at: datetime


# ── Rules ─────────────────────────────────────────────────────────────────────

class RuleIn(BaseModel):
    rule_name: str = Field(..., min_length=1, max_length=128)
    rule_type: RuleTypeEnum
    conditions: dict[str, Any] = Field(..., description="Match criteria as JSON")
    action_params: dict[str, Any] = Field(default_factory=dict)
    priority_order: int = Field(default=100, ge=1, le=1000)
    is_active: bool = True

    model_config = {"json_schema_extra": {
        "example": {
            "rule_name": "Force critical payment alerts",
            "rule_type": "force_now",
            "conditions": {"event_type": ["payment_failed", "payment_declined"]},
            "action_params": {},
            "priority_order": 1,
            "is_active": True
        }
    }}

class RuleOut(RuleIn):
    id: str
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


# ── User Profile ──────────────────────────────────────────────────────────────

class UserPreferenceUpdate(BaseModel):
    dnd_start_hour: Optional[int] = Field(None, ge=0, le=23)
    dnd_end_hour: Optional[int] = Field(None, ge=0, le=23)
    timezone: Optional[str] = None
    channel_preferences: Optional[dict[str, bool]] = None
    opted_out_topics: Optional[List[str]] = None
    hourly_cap_override: Optional[int] = Field(None, ge=1, le=100)
    daily_cap_override: Optional[int] = Field(None, ge=1, le=500)

class UserNotificationProfile(BaseModel):
    user_id: str
    notifications_last_1h: int
    notifications_last_24h: int
    dnd_active: bool
    dnd_start_hour: int
    dnd_end_hour: int
    timezone: str
    hourly_cap: int
    daily_cap: int
    opted_out_topics: List[str]
    optimal_send_hours: List[int]
    recent_decisions: List[dict]


# ── Audit ─────────────────────────────────────────────────────────────────────

class AuditEntry(BaseModel):
    event_id: str
    user_id: str
    event_type: str
    decision: DecisionEnum
    score: Optional[float]
    ai_used: bool
    fallback_used: bool
    rule_matched: Optional[str]
    reason_chain: List[ReasonStep]
    raw_event: dict
    created_at: datetime
