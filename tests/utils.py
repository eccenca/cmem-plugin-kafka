"""Testing utilities."""
import os

import pytest

# check for cmem environment and skip if not present
from _pytest.mark import MarkDecorator
from cmem_plugin_base.dataintegration.context import ExecutionContext, ReportContext, TaskContext

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
        "bootstrap_server":  os.environ.get('KAFKA_BOOTSTRAP_SERVER', ''),
        "security_protocol": os.environ.get('KAFKA_SECURITY_PROTOCOL', ''),
        "sasl_mechanisms": os.environ.get('KAFKA_SASL_MECHANISMS', ''),
        "sasl_username": os.environ.get('KAFKA_SASL_USERNAME', ''),
        "sasl_password": os.environ.get('KAFKA_SASL_PASSWORD', '')
    }


class TestExecutionContext(ExecutionContext):
    """dummy execution context that can be used in tests"""

    __test__ = False

    def __init__(self, project_id: str = "dummyProject"):
        self.report = ReportContext()
        self.task = TaskContext()
        self.task.project_id = lambda: project_id


