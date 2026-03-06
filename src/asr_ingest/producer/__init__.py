"""Producer layer - Kafka or echo backend."""

from asr_ingest.producer.base import ProducerBackend
from asr_ingest.producer.echo_producer import EchoProducer
from asr_ingest.producer.kafka_producer import KafkaProducer

__all__ = ["ProducerBackend", "KafkaProducer", "EchoProducer", "get_producer_backend"]


def get_producer_backend(
    demo_mode: bool,
    kafka_bootstrap: str = "",
    kafka_topic: str = "asr_realtime_text",
    demo_output_file: str | None = None,
) -> ProducerBackend:
    """Factory: return EchoProducer when demo_mode else KafkaProducer."""
    if demo_mode:
        return EchoProducer(output_file=demo_output_file)
    return KafkaProducer(
        bootstrap_servers=kafka_bootstrap,
        topic=kafka_topic,
    )
