# 🔔 Notification Prioritization Engine

An **AI-native engine** that evaluates every incoming notification and routes it to **Now**, **Later**, or **Never** — with a logged, human-readable reason for every decision.

> Built for Cyepro Solutions — Round 1 AI Hiring Test  
> AI tools used: Claude (design + scaffolding) — all code manually reviewed and tuned.

---

## 🏗️ Architecture

```
Incoming Event
     │
     ▼
[L0] Expiry Check ──────────────────────────────────→ NEVER (expired)
     │
     ▼
[L1] Deduplication Guard (Redis)
     ├── Tier 1: Exact SHA-256 match ───────────────→ NEVER (duplicate)
     ├── Tier 2: Near-dedup MinHash LSH ────────────→ NEVER (near-dup)
     └── Tier 3: Topic cooldown ──────────────────→  DEFER / NEVER
     │
     ▼
[L2] Rules Engine (hot-reload from Redis/Postgres)
     ├── force_now rules ─────────────────────────→  NOW (hard)
     ├── force_never rules ───────────────────────→  NEVER (hard)
     └── quiet_hours / channel_override ─────────→  LATER / NEVER
     │
     ▼
[L3] Context Enricher (parallel async fetch)
     ├── Redis: fatigue counters (1h / 24h)
     ├── Postgres: user profile / DND / caps
     └── Redis: last-send timestamps
     │
     ▼
[L4] AI Scorer ── Groq (llama-3.1-8b-instant)
     └── Fallback: Heuristic scorer (circuit breaker)
     │
     ▼
[L5] Decision Arbiter (merge all signals)
     │
     ▼
[L6] Dispatcher
     ├── NOW   → Kafka send_now_queue + fatigue counters
     ├── LATER → Kafka defer_queue + DigestBatch record
     └── NEVER → Audit log only
```

---

## 🚀 Quick Start

### Prerequisites
- Docker + Docker Compose
- Groq API key (free at [console.groq.com](https://console.groq.com))

### 1. Clone and configure
```bash
git clone <your-repo>
cd notification-engine
cp .env.example .env
# Edit .env and set GROQ_API_KEY=gsk_your_key_here
```

### 2. Start all services
```bash
docker compose up -d
```

### 3. Verify health
```bash
curl http://localhost:8000/health
```

### 4. Open API docs
```
http://localhost:8000/docs
```

### 5. Send your first notification
```bash
curl -X POST http://localhost:8000/v1/notifications/evaluate \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user_123",
    "event_type": "payment_failed",
    "title": "Payment Failed",
    "message": "Your payment of $49.99 could not be processed.",
    "source": "billing-service",
    "channel": "push",
    "priority_hint": "critical"
  }'
```

**Response:**
```json
{
  "event_id": "uuid-here",
  "user_id": "user_123",
  "decision": "now",
  "score": 0.923,
  "scheduled_at": null,
  "reason_chain": [
    {"layer": "L1-Dedup", "check": "exact_duplicate", "result": "PASS"},
    {"layer": "L2-Rules", "check": "rule:Force critical payment alerts", "result": "FORCE_NOW"},
    {"layer": "L5-Arbiter", "check": "rule_override", "result": "NOW", "detail": "..."}
  ],
  "ai_used": false,
  "fallback_used": false
}
```

---

## 📡 API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/v1/notifications/evaluate` | Evaluate a single notification |
| `POST` | `/v1/notifications/batch-evaluate` | Evaluate up to 500 events |
| `GET`  | `/v1/notifications/audit/{event_id}` | Full decision audit trail |
| `GET`  | `/v1/notifications/history/{user_id}` | User's recent decisions |
| `GET`  | `/v1/rules` | List all rules |
| `POST` | `/v1/rules` | Create a rule (no deploy needed) |
| `PUT`  | `/v1/rules/{rule_id}` | Update a rule |
| `PATCH`| `/v1/rules/{rule_id}/toggle` | Toggle rule on/off |
| `GET`  | `/v1/users/{user_id}/notification-profile` | User fatigue state |
| `PATCH`| `/v1/users/{user_id}/preferences` | Update DND, caps, opt-outs |
| `POST` | `/v1/users/{user_id}/opt-out/{topic}` | Opt out of a topic |
| `POST` | `/v1/users/{user_id}/feedback` | Record engagement feedback |
| `GET`  | `/health` | Service health check |
| `GET`  | `/metrics` | Prometheus metrics |

---

## ⚙️ Configuration

All settings via `.env`:

| Variable | Default | Description |
|----------|---------|-------------|
| `GROQ_API_KEY` | *(required)* | Your Groq API key |
| `GROQ_MODEL` | `llama-3.1-8b-instant` | Model for scoring |
| `GROQ_TIMEOUT_SECONDS` | `1.5` | AI call timeout before fallback |
| `AI_SCORE_NOW_THRESHOLD` | `0.75` | Score ≥ this → NOW |
| `AI_SCORE_LATER_THRESHOLD` | `0.40` | Score ≥ this → LATER |
| `DEFAULT_HOURLY_CAP` | `5` | Max notifications per user per hour |
| `DEFAULT_DAILY_CAP` | `20` | Max per user per day |
| `DEFAULT_COOLDOWN_SECONDS` | `3600` | Topic cooldown after send |
| `EXACT_DEDUP_TTL_SECONDS` | `3600` | Exact dedup window |
| `LSH_JACCARD_THRESHOLD` | `0.85` | Near-dedup similarity threshold |

---

## 🧠 Decision Logic

### Scoring Formula
```
S = (0.35 × Urgency) + (0.25 × Engagement) − (0.25 × FatiguePenalty) + (0.15 × RecencyBonus)
```

| Score | Decision |
|-------|----------|
| ≥ 0.75 | **NOW** |
| 0.40 – 0.74 | **LATER** |
| < 0.40 | **NEVER** |

### Fatigue Controls
- **Hourly cap**: Default 5 per channel. Configurable per user segment.
- **Daily cap**: Default 20 across all channels.
- **Topic cooldown**: 1 hour between same `event_type`.
- **DND**: Defers all non-critical during quiet hours.
- **Digest batching**: Groups deferred events into single delivery.

### AI Fallback Ladder
```
Groq timeout → Heuristic scorer (85% agreement)
Circuit open → Heuristic scorer (3 failures → 30s recovery)
Both down    → Safe default: LATER (never silently drops)
```

---

## 🧪 Running Tests

```bash
# Install deps locally
pip install -r requirements.txt

# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ -v --cov=app --cov-report=term-missing
```

---

## 📊 Monitoring

| Service | URL |
|---------|-----|
| API Docs | http://localhost:8000/docs |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 (admin/admin) |

### Key Metrics (via `/metrics`)
- `http_requests_total` — request volume by endpoint and status
- `http_request_duration_seconds` — latency histograms
- Custom: decision distribution, AI vs fallback rate, dedup hit rate

---

## 🗂️ Project Structure

```
notification-engine/
├── app/
│   ├── main.py              # FastAPI app entry point
│   ├── config.py            # Pydantic settings
│   ├── api/
│   │   ├── notifications.py # Core evaluation endpoints
│   │   ├── rules.py         # Rule CRUD
│   │   └── users.py         # User profile + preferences
│   ├── services/
│   │   ├── pipeline.py      # Pipeline orchestrator
│   │   ├── dedup.py         # 3-tier deduplication
│   │   ├── rules_engine.py  # Hot-reloadable rules
│   │   ├── context_enricher.py # User context fetcher
│   │   ├── ai_scorer.py     # Groq + heuristic fallback
│   │   ├── arbiter.py       # Decision merge logic
│   │   ├── dispatcher.py    # Routing + counter updates
│   │   └── scheduler.py     # Deferred delivery worker
│   ├── models/
│   │   ├── schemas.py       # Pydantic request/response models
│   │   ├── tables.py        # SQLAlchemy ORM tables
│   │   └── database.py      # Async DB connection
│   └── utils/
│       ├── redis_client.py  # Redis singleton + key builders
│       └── kafka_client.py  # Kafka producer/consumer
├── tests/
│   └── test_engine.py       # Full test suite
├── scripts/
│   └── seed.py              # DB seeding with sample rules/users
├── docker-compose.yml
├── Dockerfile
├── prometheus.yml
├── requirements.txt
└── .env.example
```

---

## 🔑 Creating Custom Rules (No Deployment)

```bash
curl -X POST http://localhost:8000/v1/rules \
  -H "Content-Type: application/json" \
  -d '{
    "rule_name": "Block newsletters on weekends",
    "rule_type": "force_never",
    "conditions": {
      "event_type": ["newsletter"],
      "channel": "push"
    },
    "action_params": {},
    "priority_order": 25,
    "is_active": true
  }'
```

Rule is live within **30 seconds** — no redeploy needed.

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| API | FastAPI + Uvicorn (async) |
| AI Scoring | Groq API (llama-3.1-8b-instant) |
| Fallback | Pure Python heuristic scorer |
| Event Broker | Apache Kafka |
| Cache + Dedup | Redis 7 |
| Database | PostgreSQL 15 |
| Monitoring | Prometheus + Grafana |
| Containerization | Docker Compose |
