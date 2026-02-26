"""
Notification Prioritization Engine — Main Application
"""
import structlog
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_fastapi_instrumentator import Instrumentator

from app.config import get_settings
from app.models.database import create_tables
from app.utils.redis_client import get_redis, close_redis
from app.api.notifications import router as notifications_router
from app.api.rules import router as rules_router
from app.api.users import router as users_router

settings = get_settings()
log = structlog.get_logger()


# ── Startup / Shutdown ────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage startup and shutdown tasks."""
    log.info("app.starting", env=settings.env)

    # Create DB tables
    await create_tables()
    log.info("app.db_ready")

    # Warm Redis connection
    try:
        r = await get_redis()
        await r.ping()
        log.info("app.redis_ready")
    except Exception as e:
        log.warning("app.redis_unavailable", error=str(e))

    # Seed default rules if none exist
    await _seed_default_rules()

    log.info("app.ready")
    yield

    # Cleanup
    await close_redis()
    log.info("app.shutdown")


async def _seed_default_rules():
    """Insert sensible default rules on first boot."""
    from app.models.database import AsyncSessionLocal
    from app.models.tables import RuleConfig
    from sqlalchemy import select, func

    async with AsyncSessionLocal() as db:
        result = await db.execute(select(func.count()).select_from(RuleConfig))
        count = result.scalar()
        if count and count > 0:
            return  # Rules already exist

        default_rules = [
            RuleConfig(
                rule_name="Force critical payment alerts",
                rule_type="force_now",
                conditions={"event_type": ["payment_failed", "payment_declined", "payment_error"]},
                action_params={},
                priority_order=1,
                is_active=True,
            ),
            RuleConfig(
                rule_name="Force security and auth alerts",
                rule_type="force_now",
                conditions={"event_type": ["security_alert", "login_attempt", "otp", "2fa", "password_reset"]},
                action_params={},
                priority_order=2,
                is_active=True,
            ),
            RuleConfig(
                rule_name="Suppress all promotions via SMS",
                rule_type="channel_override",
                conditions={"event_type": ["promo_offer", "promotion", "marketing", "discount", "newsletter"]},
                action_params={"allowed_channels": ["push", "email", "in_app"]},
                priority_order=10,
                is_active=True,
            ),
            RuleConfig(
                rule_name="Global quiet hours 22-08 UTC",
                rule_type="quiet_hours",
                conditions={},  # Applies to all events (non-critical filtered by arbiter)
                action_params={"start_hour": 22, "end_hour": 8},
                priority_order=20,
                is_active=True,
            ),
        ]

        for rule in default_rules:
            db.add(rule)
        await db.commit()
        log.info("app.default_rules_seeded", count=len(default_rules))


# ── Application ───────────────────────────────────────────────────────────────

app = FastAPI(
    title="Notification Prioritization Engine",
    description=(
        "AI-native engine that decides whether each notification should be "
        "sent **Now**, **Later**, or **Never** — with full explainability."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# ── Middleware ─────────────────────────────────────────────────────────────────

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Prometheus Metrics ─────────────────────────────────────────────────────────

Instrumentator().instrument(app).expose(app, endpoint="/metrics")

# ── Routers ───────────────────────────────────────────────────────────────────

app.include_router(notifications_router)
app.include_router(rules_router)
app.include_router(users_router)


# ── Health & Info ─────────────────────────────────────────────────────────────

@app.get("/health", tags=["System"])
async def health_check():
    checks = {}

    # Redis
    try:
        r = await get_redis()
        await r.ping()
        checks["redis"] = "ok"
    except Exception as e:
        checks["redis"] = f"error: {str(e)}"

    # DB
    try:
        from app.models.database import engine
        async with engine.connect() as conn:
            await conn.execute(__import__("sqlalchemy").text("SELECT 1"))
        checks["postgres"] = "ok"
    except Exception as e:
        checks["postgres"] = f"error: {str(e)}"

    all_ok = all(v == "ok" for v in checks.values())
    return JSONResponse(
        status_code=200 if all_ok else 503,
        content={
            "status": "healthy" if all_ok else "degraded",
            "version": "1.0.0",
            "services": checks,
        },
    )


@app.get("/", tags=["System"])
async def root():
    return {
        "name": "Notification Prioritization Engine",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health",
        "metrics": "/metrics",
    }


# ── Global Error Handler ──────────────────────────────────────────────────────

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    log.error("app.unhandled_exception", path=request.url.path, error=str(exc))
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error", "path": str(request.url.path)},
    )
