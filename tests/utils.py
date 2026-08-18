"""Testing utilities."""

import os
from pathlib import Path

import pytest

# check for cmem environment and skip if not present
from _pytest.mark import MarkDecorator
from cmem_client.client import Client
from cmem_plugin_base.testing import TestExecutionContext, TestPluginContext, TestUserContext
from defusedxml import ElementTree

__all__ = [
    "FIXTURES_DIR",
    "TestExecutionContext",
    "TestPluginContext",
    "TestUserContext",
    "XMLUtils",
    "get_client",
    "get_kafka_config",
    "needs_cmem",
    "needs_kafka",
]

FIXTURES_DIR = Path(__file__).parent / "fixtures"

needs_cmem: MarkDecorator = pytest.mark.skipif(
    "CMEM_BASE_URI" not in os.environ, reason="Needs CMEM configuration"
)

needs_kafka: MarkDecorator = pytest.mark.skipif(
    "KAFKA_BOOTSTRAP_SERVER" not in os.environ,
    "KAFKA_SECURITY_PROTOCOL" not in os.environ,
    reason="Needs Kafka service configuration",
)


def get_kafka_config() -> dict:
    """To get the kafka configuration from environment variables"""
    return {
        "bootstrap_server": os.environ.get("KAFKA_BOOTSTRAP_SERVER", ""),
        "security_protocol": os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
        "sasl_mechanisms": os.environ.get("KAFKA_SASL_MECHANISMS", ""),
        "sasl_username": os.environ.get("KAFKA_SASL_USERNAME", ""),
        "sasl_password": os.environ.get("KAFKA_SASL_PASSWORD", ""),
    }


def get_client(project_id: str = "dummyProject") -> Client:
    """Get a fresh client

    Clients are created per operation on purpose: a client keeps its HTTP connections
    alive in a pool, and a connection which idles while messages are produced or
    consumed is closed by the server before it is used again.
    """
    return Client.from_context(context=TestExecutionContext(project_id=project_id))


class XMLUtils:
    """Standard xml utils class for testing"""

    @staticmethod
    def get_elements_len_fromstring(content: str) -> int:
        """Return elements len from xml string data"""
        tree = ElementTree.fromstring(content)
        # returns the elements from depth level 1
        return len(tree.findall("./"))

    @staticmethod
    def get_elements_len_from_file(path: str) -> int:
        """Return elements len of xml file"""
        tree = ElementTree.parse(path).getroot()
        return len(tree.findall("./"))
