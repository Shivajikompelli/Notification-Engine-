"""
Test suite for the Notification Prioritization Engine.
Run: pytest tests/ -v
"""
import pytest
import asyncio
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_event_critical():
    from app.models.schemas import NotificationEventIn
    return NotificationEventIn(
        user_id="user_test_001",
        event_type="payment_failed",
        title="Payment Failed",
        message="Your payment of $49.99 could not be processed. Please update your billing details.",
        source="billing-service",
        channel="push",
        priority_hint="critical",
        metadata={"amount": 49.99, "currency": "USD"},
    )


@pytest.fixture
def sample_event_promo():
    from app.models.schemas import NotificationEventIn
    return NotificationEventIn(
        user_id="user_test_002",
        event_type="promo_offer",
        title="50% Off This Weekend!",
        message="Don't miss our biggest sale of the year. Shop now and save big!",
        source="marketing-service",
        channel="push",
        priority_hint="low",
    )


@pytest.fixture
def sample_event_message():
    from app.models.schemas import NotificationEventIn
    return NotificationEventIn(
        user_id="user_test_003",
        event_type="new_message",
        title="You have a new message",
        message="Alice sent you a message: 'Hey, are you available for a call?'",
        source="messaging-service",
        channel="push",
        priority_hint="high",
    )


@pytest.fixture
def sample_event_expired():
    from app.models.schemas import NotificationEventIn
    from datetime import timezone
    return NotificationEventIn(
        user_id="user_test_004",
        event_type="flash_sale",
        title="Flash Sale - 2 Hours Only!",
        message="Use code FLASH50 for 50% off. Offer expires soon!",
        source="promotions",
        channel="push",
        priority_hint="medium",
        expires_at=datetime.now(timezone.utc) - timedelta(hours=1),  # Already expired
    )


# ─────────────────────────────────────────────────────────────────────────────
# Deduplication Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestDeduplication:

    def test_fingerprint_deterministic(self, sample_event_critical):
        from app.services.dedup import _compute_fingerprint
        fp1 = _compute_fingerprint(sample_event_critical)
        fp2 = _compute_fingerprint(sample_event_critical)
        assert fp1 == fp2, "Fingerprint must be deterministic"

    def test_fingerprint_different_users(self, sample_event_critical):
        from app.services.dedup import _compute_fingerprint
        from app.models.schemas import NotificationEventIn
        event2 = sample_event_critical.model_copy(update={"user_id": "different_user"})
        fp1 = _compute_fingerprint(sample_event_critical)
        fp2 = _compute_fingerprint(event2)
        assert fp1 != fp2, "Different users must produce different fingerprints"

    def test_fingerprint_uses_dedupe_key(self):
        from app.services.dedup import _compute_fingerprint
        from app.models.schemas import NotificationEventIn
        event = NotificationEventIn(
            user_id="u1", event_type="test", title="Title A", message="msg",
            source="svc", dedupe_key="stable-key-123"
        )
        event2 = NotificationEventIn(
            user_id="u1", event_type="test", title="Title B DIFFERENT", message="msg",
            source="svc", dedupe_key="stable-key-123"
        )
        assert _compute_fingerprint(event) == _compute_fingerprint(event2), \
            "Same dedupe_key must produce same fingerprint"

    def test_minhash_similar_texts(self):
        from app.services.dedup import _compute_minhash, _jaccard_from_lists, _minhash_to_list
        text1 = "Your payment of $49 failed. Please update billing details."
        text2 = "Your payment of $49 has failed. Please update your billing details."
        mh1 = _minhash_to_list(_compute_minhash(text1))
        mh2 = _minhash_to_list(_compute_minhash(text2))
        similarity = _jaccard_from_lists(mh1, mh2)
        assert similarity > 0.7, f"Similar texts should have high Jaccard: {similarity}"

    def test_minhash_different_texts(self):
        from app.services.dedup import _compute_minhash, _jaccard_from_lists, _minhash_to_list
        text1 = "Payment failed update billing"
        text2 = "New message from your friend Alice about the weekend"
        mh1 = _minhash_to_list(_compute_minhash(text1))
        mh2 = _minhash_to_list(_compute_minhash(text2))
        similarity = _jaccard_from_lists(mh1, mh2)
        assert similarity < 0.5, f"Different texts should have low Jaccard: {similarity}"

    def test_normalize_text(self):
        from app.services.dedup import _normalize_text
        assert _normalize_text("Hello, World!") == "hello world"
        assert _normalize_text("  Extra   Spaces  ") == "extra spaces"


# ─────────────────────────────────────────────────────────────────────────────
# Rules Engine Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestRulesEngine:

    def test_match_list_condition(self, sample_event_critical):
        from app.services.rules_engine import _matches_conditions
        conditions = {"event_type": ["payment_failed", "payment_declined"]}
        assert _matches_conditions(sample_event_critical, conditions) is True

    def test_no_match_list_condition(self, sample_event_promo):
        from app.services.rules_engine import _matches_conditions
        conditions = {"event_type": ["payment_failed", "payment_declined"]}
        assert _matches_conditions(sample_event_promo, conditions) is False

    def test_match_exact_condition(self, sample_event_promo):
        from app.services.rules_engine import _matches_conditions
        conditions = {"channel": "push", "priority_hint": "low"}
        assert _matches_conditions(sample_event_promo, conditions) is True

    def test_match_contains_operator(self, sample_event_critical):
        from app.services.rules_engine import _matches_conditions
        conditions = {"event_type": {"contains": "payment"}}
        assert _matches_conditions(sample_event_critical, conditions) is True

    def test_quiet_hours_overnight(self):
        from app.services.rules_engine import _is_quiet_hours
        # 22:00 - 08:00, check at 23:00 → should be quiet
        params = {"start_hour": 22, "end_hour": 8}
        with patch("app.services.rules_engine.datetime") as mock_dt:
            mock_dt.utcnow.return_value = datetime(2024, 1, 1, 23, 0)
            assert _is_quiet_hours(params) is True

    def test_quiet_hours_not_active(self):
        from app.services.rules_engine import _is_quiet_hours
        params = {"start_hour": 22, "end_hour": 8}
        with patch("app.services.rules_engine.datetime") as mock_dt:
            mock_dt.utcnow.return_value = datetime(2024, 1, 1, 14, 0)
            assert _is_quiet_hours(params) is False


# ─────────────────────────────────────────────────────────────────────────────
# Context Enricher Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestContextEnricher:

    def test_dnd_active_overnight(self):
        from app.services.context_enricher import _is_dnd_active
        assert _is_dnd_active(22, 8, 23) is True  # 23:00 in 22-08 window
        assert _is_dnd_active(22, 8, 7) is True   # 07:00 in 22-08 window
        assert _is_dnd_active(22, 8, 14) is False  # 14:00 outside window

    def test_dnd_not_active_daytime(self):
        from app.services.context_enricher import _is_dnd_active
        assert _is_dnd_active(22, 8, 10) is False
        assert _is_dnd_active(22, 8, 20) is False

    def test_user_context_fatigue_ratio(self):
        from app.services.context_enricher import UserContext
        ctx = UserContext(user_id="u1", notifications_last_1h=4, hourly_cap=5)
        assert ctx.fatigue_ratio_1h == pytest.approx(0.8)
        assert ctx.hourly_cap_hit is False

    def test_user_context_cap_hit(self):
        from app.services.context_enricher import UserContext
        ctx = UserContext(user_id="u1", notifications_last_1h=5, hourly_cap=5)
        assert ctx.hourly_cap_hit is True

    def test_recency_bonus_never_sent(self):
        from app.services.context_enricher import UserContext
        ctx = UserContext(user_id="u1", seconds_since_last_same_type=None)
        assert ctx.recency_bonus == 1.0

    def test_recency_bonus_just_sent(self):
        from app.services.context_enricher import UserContext
        ctx = UserContext(user_id="u1", seconds_since_last_same_type=0)
        assert ctx.recency_bonus == pytest.approx(0.0, abs=0.01)


# ─────────────────────────────────────────────────────────────────────────────
# Heuristic Scorer Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestHeuristicScorer:

    def test_critical_event_scores_high(self, sample_event_critical):
        from app.services.ai_scorer import _heuristic_score
        from app.services.context_enricher import UserContext
        ctx = UserContext(user_id="u1")
        result = _heuristic_score(sample_event_critical, ctx)
        assert result.score >= 0.70, f"Critical event should score >= 0.70, got {result.score}"
        assert result.decision_hint in ("now", "later")

    def test_promo_event_scores_low(self, sample_event_promo):
        from app.services.ai_scorer import _heuristic_score
        from app.services.context_enricher import UserContext
        ctx = UserContext(user_id="u1", notifications_last_1h=4, hourly_cap=5)
        result = _heuristic_score(sample_event_promo, ctx)
        assert result.score <= 0.50, f"Promo should score <= 0.50, got {result.score}"

    def test_fallback_flag_set(self, sample_event_promo):
        from app.services.ai_scorer import _heuristic_score
        from app.services.context_enricher import UserContext
        ctx = UserContext(user_id="u1")
        result = _heuristic_score(sample_event_promo, ctx, fallback_reason="test")
        assert result.fallback_used is True
        assert result.ai_used is False

    def test_score_bounded(self, sample_event_critical):
        from app.services.ai_scorer import _heuristic_score
        from app.services.context_enricher import UserContext
        # Max fatigue
        ctx = UserContext(
            user_id="u1", notifications_last_1h=10, hourly_cap=5,
            engagement_heatmap=[0.0] * 24,
        )
        result = _heuristic_score(sample_event_critical, ctx)
        assert 0.0 <= result.score <= 1.0


# ─────────────────────────────────────────────────────────────────────────────
# Arbiter Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestArbiter:

    def _make_ai_result(self, score: float, decision: str = "later"):
        from app.services.ai_scorer import ScoringResult
        return ScoringResult(
            score=score, decision_hint=decision,
            urgency=0.5, engagement=0.5, fatigue_penalty=0.0, recency_bonus=0.5,
            reasoning="test", ai_used=True, fallback_used=False,
        )

    def _dummy_steps(self):
        from app.models.schemas import ReasonStep
        return [ReasonStep(layer="test", check="test", result="PASS")]

    def test_rule_force_now_wins(self, sample_event_critical):
        from app.services.arbiter import arbitrate
        from app.services.context_enricher import UserContext
        ctx = UserContext(user_id="u1")
        ai = self._make_ai_result(0.1)  # Even low score
        decision, scheduled_at, _, _ = arbitrate(
            sample_event_critical, "now", "test_rule", ai, ctx,
            self._dummy_steps(), self._dummy_steps(), self._dummy_steps()[0]
        )
        from app.models.schemas import DecisionEnum
        assert decision == DecisionEnum.now

    def test_rule_force_never_wins(self, sample_event_promo):
        from app.services.arbiter import arbitrate
        from app.services.context_enricher import UserContext
        ctx = UserContext(user_id="u1")
        ai = self._make_ai_result(0.9)  # Even high score
        decision, _, _, _ = arbitrate(
            sample_event_promo, "never", "suppress_promos", ai, ctx,
            self._dummy_steps(), self._dummy_steps(), self._dummy_steps()[0]
        )
        from app.models.schemas import DecisionEnum
        assert decision == DecisionEnum.never

    def test_high_score_sends_now(self, sample_event_message):
        from app.services.arbiter import arbitrate
        from app.services.context_enricher import UserContext
        ctx = UserContext(user_id="u1", dnd_active=False)
        ai = self._make_ai_result(0.90, "now")
        decision, _, _, _ = arbitrate(
            sample_event_message, None, None, ai, ctx,
            [], [], self._dummy_steps()[0]
        )
        from app.models.schemas import DecisionEnum
        assert decision == DecisionEnum.now

    def test_low_score_never(self, sample_event_promo):
        from app.services.arbiter import arbitrate
        from app.services.context_enricher import UserContext
        ctx = UserContext(user_id="u1")
        ai = self._make_ai_result(0.10, "never")
        decision, _, _, _ = arbitrate(
            sample_event_promo, None, None, ai, ctx,
            [], [], self._dummy_steps()[0]
        )
        from app.models.schemas import DecisionEnum
        assert decision == DecisionEnum.never

    def test_dnd_defers_non_critical(self, sample_event_message):
        from app.services.arbiter import arbitrate
        from app.services.context_enricher import UserContext
        ctx = UserContext(user_id="u1", dnd_active=True)
        ai = self._make_ai_result(0.80, "now")
        decision, scheduled_at, _, _ = arbitrate(
            sample_event_message, None, None, ai, ctx,
            [], [], self._dummy_steps()[0]
        )
        from app.models.schemas import DecisionEnum
        assert decision == DecisionEnum.later
        assert scheduled_at is not None

    def test_critical_bypasses_dnd(self, sample_event_critical):
        from app.services.arbiter import arbitrate
        from app.services.context_enricher import UserContext
        ctx = UserContext(user_id="u1", dnd_active=True)
        ai = self._make_ai_result(0.95, "now")
        decision, _, _, _ = arbitrate(
            sample_event_critical, None, None, ai, ctx,
            [], [], self._dummy_steps()[0]
        )
        from app.models.schemas import DecisionEnum
        assert decision == DecisionEnum.now


# ─────────────────────────────────────────────────────────────────────────────
# Schema Validation Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestSchemaValidation:

    def test_invalid_expires_at_raises(self):
        from app.models.schemas import NotificationEventIn
        from datetime import timezone
        import pydantic
        with pytest.raises(pydantic.ValidationError):
            NotificationEventIn(
                user_id="u1",
                event_type="test",
                title="T",
                message="M",
                source="s",
                expires_at=datetime.now(timezone.utc) - timedelta(hours=2),
            )

    def test_valid_event_passes(self, sample_event_critical):
        assert sample_event_critical.user_id == "user_test_001"
        assert sample_event_critical.priority_hint.value == "critical"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
