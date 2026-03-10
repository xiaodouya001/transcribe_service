"""Producer layer - Kafka backend."""

from transcription_ingest.producer.base import ProducerBackend
from transcription_ingest.producer.kafka_producer import KafkaProducer

__all__ = ["ProducerBackend", "KafkaProducer", "get_producer_backend"]


def get_producer_backend(
    kafka_bootstrap: str = "",
    kafka_topic: str = "transcription_topic",
    *,
    compression_type: str = "none",
    send_timeout_sec: float = 10.0,
) -> ProducerBackend:
    """Factory: return KafkaProducer."""
    return KafkaProducer(
        bootstrap_servers=kafka_bootstrap,
        topic=kafka_topic,
        compression_type=compression_type,
        send_timeout_sec=send_timeout_sec,
    )
