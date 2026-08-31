"""test configuration module"""

import logging
import secrets
from collections.abc import Generator

import pytest
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic
from testcontainers.community.kafka import KafkaContainer

from .utils import KAFKA_CONFIG

TOPIC_PREFIX = "cmem"


@pytest.fixture(scope="session")
def kafka_broker() -> Generator[KafkaContainer]:
    """Start a single KRaft-mode Kafka broker for the whole test session.

    Always started fresh and always torn down at the end of the run - no reuse
    detection of an already-running broker, no opt-out.
    """
    with KafkaContainer().with_kraft() as broker:
        KAFKA_CONFIG["bootstrap_server"] = broker.get_bootstrap_server()
        yield broker


@pytest.fixture
def topic(kafka_broker: KafkaContainer) -> Generator:
    """Create a test topic"""
    kafka_service = KAFKA_CONFIG["bootstrap_server"]
    a = AdminClient({"bootstrap.servers": kafka_service})
    default_topic = f"{TOPIC_PREFIX}_{secrets.randbelow(1000)}"
    new_topics = [NewTopic(topic, num_partitions=1) for topic in [default_topic]]
    fs = a.create_topics(new_topics)

    # Wait for each operation to finish.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            logging.getLogger(__name__).info(f"Topic {topic} created")
        except Exception:
            logging.getLogger(__name__).exception(f"Failed to create topic {topic}")
    yield default_topic
    fs = a.delete_topics([default_topic], operation_timeout=30)

    # Wait for operation to finish.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            logging.getLogger(__name__).info(f"Topic {topic} deleted")
        except Exception:
            logging.getLogger(__name__).exception(f"Failed to delete topic {topic}")
