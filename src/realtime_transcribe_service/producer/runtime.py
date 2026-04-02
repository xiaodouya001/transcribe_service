"""Runtime wiring helpers for Kafka producer dependencies."""

from __future__ import annotations

from realtime_transcribe_service.config.settings import Settings
from realtime_transcribe_service.producer.kafka_connection import kafka_connection_for_mode
from realtime_transcribe_service.producer.kafka_producer import KafkaProducer


def create_kafka_producer(settings: Settings) -> KafkaProducer:
    """Create the Kafka producer for the configured runtime mode."""
    bootstrap_servers = settings.kafka_bootstrap_servers
    assert bootstrap_servers is not None
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        topic=settings.kafka_topic,
        connection=kafka_connection_for_mode(
            settings.kafka_mode,
            aws_region=settings.kafka_aws_region,
            ssl_ca_file=settings.kafka_ssl_ca_file,
            aws_debug_creds=settings.kafka_aws_debug_creds,
        ),
        compression_type=settings.kafka_compression_type,
        send_timeout_sec=settings.kafka_send_timeout_sec,
        linger_ms=settings.kafka_linger_ms,
        batch_size=settings.kafka_batch_size,
    )
