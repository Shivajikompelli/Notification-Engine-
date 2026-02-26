from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # App
    app_name: str = "Notification Prioritization Engine"
    env: str = "development"
    log_level: str = "INFO"
    api_secret_key: str = "dev_secret"

    # Database
    database_url: str = "postgresql+asyncpg://npe_user:npe_pass@localhost:5432/npe_db"

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_raw: str = "raw_notification_events"
    kafka_topic_evaluated: str = "evaluated_notification_events"
    kafka_topic_send_now: str = "send_now_queue"
    kafka_topic_defer: str = "defer_queue"

    # Groq AI
    groq_api_key: str = ""
    groq_model: str = "llama-3.1-8b-instant"
    groq_timeout_seconds: float = 1.5

    # Decision thresholds
    ai_score_now_threshold: float = 0.75
    ai_score_later_threshold: float = 0.40

    # Fatigue defaults
    default_hourly_cap: int = 5
    default_daily_cap: int = 20
    default_cooldown_seconds: int = 3600

    # Dedup
    exact_dedup_ttl_seconds: int = 3600
    near_dedup_ttl_seconds: int = 86400
    lsh_jaccard_threshold: float = 0.85
    lsh_num_perm: int = 128

    # Scheduler
    scheduler_poll_interval_seconds: int = 30
    digest_batch_window_minutes: int = 30

    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    return Settings()
