from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from app.config import get_settings
import structlog
import json

log = structlog.get_logger()
settings = get_settings()

_producer: AIOKafkaProducer | None = None


async def get_producer() -> AIOKafkaProducer:
    global _producer
    if _producer is None:
        _producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            enable_idempotence=True,
            max_batch_size=65536,
            linger_ms=5,
        )
        await _producer.start()
        log.info("kafka.producer_started")
    return _producer


async def close_producer():
    global _producer
    if _producer:
        await _producer.stop()
        _producer = None


async def publish(topic: str, payload: dict, key: str | None = None):
    """Publish a message to a Kafka topic. Fails silently (logged) to not block API."""
    try:
        producer = await get_producer()
        await producer.send_and_wait(topic, value=payload, key=key)
    except Exception as e:
        log.warning("kafka.publish_failed", topic=topic, error=str(e))


def make_consumer(topic: str, group_id: str) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=group_id,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
