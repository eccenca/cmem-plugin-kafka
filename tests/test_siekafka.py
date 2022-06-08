"""Plugin tests."""
import pytest
import requests
from cmem_plugin_base.dataintegration.utils import setup_cmempy_super_user_access

from cmem_plugin_siekafka.config import (
    BOOTSTRAP_SERVER,
    SECURITY_PROTOCOL,
    SASL_MECHANISMS,
    SASL_U,
    SASL_P,
    TOPIC
)
from cmem_plugin_siekafka import KafkaPlugin
from .utils import needs_cmem


def test_execution_dataset():
    """Test plugin execution"""
    plugin = KafkaPlugin(
        message_dataset='sample',
        bootstrap_servers=BOOTSTRAP_SERVER,
        security_protocol=SECURITY_PROTOCOL,
        sasl_mechanisms=SASL_MECHANISMS,
        sasl_username=SASL_U,
        sasl_password=SASL_P,
        kafka_topic=TOPIC
    )
    with pytest.raises(ValueError):
        plugin.execute()


@needs_cmem
def test_execution_valid():
    """Test plugin execution"""
    plugin = KafkaPlugin(
        message_dataset='kafka-producer:kafka-message-duplicates.xml',
        bootstrap_servers=BOOTSTRAP_SERVER,
        security_protocol=SECURITY_PROTOCOL,
        sasl_mechanisms=SASL_MECHANISMS,
        sasl_username=SASL_U,
        sasl_password=SASL_P,
        kafka_topic=TOPIC
    )
    with pytest.raises(requests.exceptions.HTTPError):
        setup_cmempy_super_user_access()
        plugin.execute()
