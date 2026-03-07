"""Producer layer - Kafka backend."""

from asr_ingest.producer.base import ProducerBackend
from asr_ingest.producer.kafka_producer import KafkaProducer

__all__ = ["ProducerBackend", "KafkaProducer", "get_producer_backend"]


def get_producer_backend(
    kafka_bootstrap: str = "",
    kafka_topic: str = "asr_realtime_text",
    *,
    compression_type: str = "none",
) -> ProducerBackend:
    """Factory: return KafkaProducer."""
    return KafkaProducer(
        bootstrap_servers=kafka_bootstrap,
        topic=kafka_topic,
        compression_type=compression_type,
    )
