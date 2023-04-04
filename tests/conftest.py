import random

import pytest
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic

from .utils import get_kafka_config

KAFKA_CONFIG = get_kafka_config()
TOPIC_PREFIX = "cmem"


@pytest.fixture
def topic():
    kafka_service = KAFKA_CONFIG["bootstrap_server"]
    a = AdminClient({"bootstrap.servers": kafka_service})
    default_topic = f"{TOPIC_PREFIX}_{random.randint(0, 1000)}"
    new_topics = [NewTopic(topic, num_partitions=1) for topic in [default_topic]]
    fs = a.create_topics(new_topics)

    # Wait for each operation to finish.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic {} created".format(topic))
        except Exception as e:
            print("Failed to create topic {}: {}".format(topic, e))
    yield default_topic
    fs = a.delete_topics([default_topic], operation_timeout=30)

    # Wait for operation to finish.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic {} deleted".format(topic))
        except Exception as e:
            print("Failed to delete topic {}: {}".format(topic, e))
