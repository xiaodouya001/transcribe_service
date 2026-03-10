"""Producer layer - Kafka backend."""

from transcribe_service.producer.base import ProducerBackend
from transcribe_service.producer.kafka_producer import KafkaProducer

__all__ = ["ProducerBackend", "KafkaProducer", "get_producer_backend"]


def get_producer_backend(
    kafka_bootstrap: str = "",
    kafka_topic: str = "transcription_topic",
    *,
    compression_type: str = "none",
    send_timeout_sec: float = 10.0,
    num_partitions: int = 6,
) -> ProducerBackend:
    """Factory: return KafkaProducer."""
    return KafkaProducer(
        bootstrap_servers=kafka_bootstrap,
        topic=kafka_topic,
        compression_type=compression_type,
        send_timeout_sec=send_timeout_sec,
        num_partitions=num_partitions,
    )
