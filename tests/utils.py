"""Testing utilities."""
import os

import pytest

# check for cmem environment and skip if not present
from _pytest.mark import MarkDecorator

needs_cmem: MarkDecorator = pytest.mark.skipif(
    "CMEM_BASE_URI" not in os.environ, reason="Needs CMEM configuration"
)

needs_kafka: MarkDecorator = pytest.mark.skipif(
    "KAFKA_BOOTSTRAP_SERVER" not in os.environ,
    "KAFKA_SECURITY_PROTOCOL" not in os.environ,
    reason="Needs Kafka service configuration"
)


def get_kafka_config():
    """To get the kafka configuration from environment variables"""
    return {
        "bootstrap_server":  os.environ["KAFKA_BOOTSTRAP_SERVER"] if "KAFKA_BOOTSTRAP_SERVER" in os.environ else '',
        "security_protocol": os.environ["KAFKA_SECURITY_PROTOCOL"] if "KAFKA_SECURITY_PROTOCOL" in os.environ else '',
        "sasl_mechanisms": os.environ["KAFKA_SASL_MECHANISMS"] if "KAFKA_SASL_MECHANISMS" in os.environ else '',
        "sasl_username": os.environ["KAFKA_SASL_USERNAME"] if "KAFKA_SASL_USERNAME" in os.environ else '',
        "sasl_password": os.environ["KAFKA_SASL_PASSWORD"] if "KAFKA_SASL_PASSWORD" in os.environ else ''
    }
