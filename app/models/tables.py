from sqlalchemy import (
    Column, String, Text, Float, Boolean, Integer,
    DateTime, JSON, Enum as SAEnum, ForeignKey, Index
)
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.dialects.postgresql import UUID, JSONB
from datetime import datetime
import uuid


class Base(DeclarativeBase):
    pass


class NotificationEvent(Base):
    __tablename__ = "notification_events"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String(64), nullable=False, index=True)
    event_type = Column(String(128), nullable=False, index=True)
    title = Column(Text, nullable=False)
    message = Column(Text, nullable=False)
    source = Column(String(64), nullable=False)
    channel = Column(String(20), nullable=False, default="push")
    priority_hint = Column(String(20), nullable=True)
    dedupe_key = Column(String(256), nullable=True)
    computed_fingerprint = Column(String(64), nullable=False, index=True)
    expires_at = Column(DateTime, nullable=True)
    event_timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    metadata_ = Column("metadata", JSONB, default=dict)

    # Decision
    decision = Column(String(10), nullable=True)  # now / later / never
    score = Column(Float, nullable=True)
    scheduled_at = Column(DateTime, nullable=True)
    decision_reason = Column(JSONB, default=list)
    ai_used = Column(Boolean, default=False)
    fallback_used = Column(Boolean, default=False)
    rule_matched = Column(String(128), nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)
    processed_at = Column(DateTime, nullable=True)

    __table_args__ = (
        Index("ix_nev_user_decision", "user_id", "decision"),
        Index("ix_nev_user_type", "user_id", "event_type"),
    )


class RuleConfig(Base):
    __tablename__ = "rule_configs"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    rule_name = Column(String(128), nullable=False, unique=True)
    rule_type = Column(String(32), nullable=False)
    conditions = Column(JSONB, nullable=False)
    action_params = Column(JSONB, default=dict)
    priority_order = Column(Integer, default=100)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_rule_active_priority", "is_active", "priority_order"),
    )


class UserProfile(Base):
    __tablename__ = "user_profiles"

    user_id = Column(String(64), primary_key=True)
    timezone = Column(String(64), default="UTC")
    dnd_start_hour = Column(Integer, default=22)
    dnd_end_hour = Column(Integer, default=8)
    channel_preferences = Column(JSONB, default=dict)
    opted_out_topics = Column(JSONB, default=list)
    hourly_cap_override = Column(Integer, nullable=True)
    daily_cap_override = Column(Integer, nullable=True)
    segment = Column(String(32), default="standard")
    # Engagement heatmap: 24 floats (one per hour) for optimal send time
    engagement_heatmap = Column(JSONB, default=lambda: [1.0] * 24)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class AuditLog(Base):
    __tablename__ = "audit_log"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    event_id = Column(String(36), nullable=False, index=True)
    user_id = Column(String(64), nullable=False, index=True)
    event_type = Column(String(128), nullable=False)
    decision = Column(String(10), nullable=False)
    score = Column(Float, nullable=True)
    ai_used = Column(Boolean, default=False)
    fallback_used = Column(Boolean, default=False)
    rule_matched = Column(String(128), nullable=True)
    reason_chain = Column(JSONB, default=list)
    raw_event = Column(JSONB, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)


class SuppressionRecord(Base):
    __tablename__ = "suppression_records"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String(64), nullable=False, index=True)
    fingerprint = Column(String(64), nullable=False)
    reason = Column(String(128), nullable=False)
    suppressed_until = Column(DateTime, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_sup_user_fingerprint", "user_id", "fingerprint"),
    )


class AIInteractionLog(Base):
    __tablename__ = "ai_interaction_logs"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    event_id = Column(String(36), nullable=False, index=True)
    user_id = Column(String(64), nullable=False, index=True)
    event_type = Column(String(128), nullable=False)

    # What was sent to Groq
    prompt = Column(Text, nullable=False)

    # What Groq returned
    response = Column(JSONB, nullable=True)       # raw JSON from Groq
    ai_used = Column(Boolean, default=True)        # False = heuristic fallback
    fallback_reason = Column(String(128), nullable=True)  # why fallback was used

    # Parsed scores
    score = Column(Float, nullable=True)
    decision = Column(String(10), nullable=True)
    urgency = Column(Float, nullable=True)
    engagement = Column(Float, nullable=True)
    fatigue_penalty = Column(Float, nullable=True)
    recency_bonus = Column(Float, nullable=True)
    reasoning = Column(Text, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, index=True)

    __table_args__ = (
        Index("ix_ai_log_user", "user_id", "created_at"),
    )


class DigestBatch(Base):
    __tablename__ = "digest_batches"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String(64), nullable=False, index=True)
    channel = Column(String(20), nullable=False)
    event_ids = Column(JSONB, default=list)
    scheduled_at = Column(DateTime, nullable=False, index=True)
    sent_at = Column(DateTime, nullable=True)
    status = Column(String(20), default="pending")  # pending / sent / cancelled
    created_at = Column(DateTime, default=datetime.utcnow)