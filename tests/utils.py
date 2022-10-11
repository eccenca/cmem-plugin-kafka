"""Testing utilities."""
import os
from defusedxml import ElementTree
import pytest

# check for cmem environment and skip if not present
from _pytest.mark import MarkDecorator
from cmem.cmempy.api import get_token
from cmem_plugin_base.dataintegration.context import (
    ExecutionContext,
    ReportContext,
    TaskContext,
    UserContext,
    PluginContext,
)

needs_cmem: MarkDecorator = pytest.mark.skipif(
    "CMEM_BASE_URI" not in os.environ, reason="Needs CMEM configuration"
)

needs_kafka: MarkDecorator = pytest.mark.skipif(
    "KAFKA_BOOTSTRAP_SERVER" not in os.environ,
    "KAFKA_SECURITY_PROTOCOL" not in os.environ,
    reason="Needs Kafka service configuration",
)


def get_kafka_config():
    """To get the kafka configuration from environment variables"""
    return {
        "bootstrap_server": os.environ.get("KAFKA_BOOTSTRAP_SERVER", ""),
        "security_protocol": os.environ.get("KAFKA_SECURITY_PROTOCOL", ""),
        "sasl_mechanisms": os.environ.get("KAFKA_SASL_MECHANISMS", ""),
        "sasl_username": os.environ.get("KAFKA_SASL_USERNAME", ""),
        "sasl_password": os.environ.get("KAFKA_SASL_PASSWORD", ""),
    }


class TestUserContext(UserContext):
    """dummy user context that can be used in tests"""

    __test__ = False

    def __init__(self):
        # get access token from default service account
        access_token = os.environ.get("OAUTH_ACCESS_TOKEN", "")
        if not access_token:
            access_token = get_token()["access_token"]
        self.token = lambda: access_token


class TestPluginContext(PluginContext):
    """dummy plugin context that can be used in tests"""

    __test__ = False

    def __init__(
            self,
            project_id: str = "dummyProject",
    ):
        self.project_id = project_id
        self.user = TestUserContext()


class TestTaskContext(TaskContext):
    """dummy Task context that can be used in tests"""

    __test__ = False

    def __init__(self, project_id: str = "dummyProject"):
        self.project_id = lambda: project_id


class TestExecutionContext(ExecutionContext):
    """dummy execution context that can be used in tests"""

    __test__ = False

    def __init__(
            self,
            project_id: str = "dummyProject",
    ):
        self.report = ReportContext()
        self.task = TestTaskContext(project_id=project_id)
        self.user = TestUserContext()


def xml_record_len(path: str):
    """Return record len of xml file"""
    tree = ElementTree.parse(path)
    root = tree.getroot()
    return len(root.findall('./Message'))