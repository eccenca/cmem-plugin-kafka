"""Testing utilities."""
import os
from typing import ClassVar
from xml.sax.expatreader import AttributesImpl
from xml.sax.handler import ContentHandler

import pytest

# check for cmem environment and skip if not present
from _pytest.mark import MarkDecorator
from cmem.cmempy.api import get_token
from cmem.cmempy.config import get_oauth_default_credentials
from cmem_plugin_base.dataintegration.context import (
    ExecutionContext,
    PluginContext,
    ReportContext,
    TaskContext,
    UserContext,
)
from defusedxml import ElementTree, sax

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
        "security_protocol": os.environ.get("KAFKA_SECURITY_PROTOCOL", ""),
        "sasl_mechanisms": os.environ.get("KAFKA_SASL_MECHANISMS", ""),
        "sasl_username": os.environ.get("KAFKA_SASL_USERNAME", ""),
        "sasl_password": os.environ.get("KAFKA_SASL_PASSWORD", ""),
    }


class TestUserContext(UserContext):
    """dummy user context that can be used in tests"""

    __test__ = False
    default_credential: ClassVar[dict] = {}

    def __init__(self):
        # get access token from default service account
        if not TestUserContext.default_credential:
            TestUserContext.default_credential = get_oauth_default_credentials()
        access_token = get_token(_oauth_credentials=TestUserContext.default_credential)[
            "access_token"
        ]
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

    def __init__(self, project_id: str = "dummyProject", task_id: str = "dummyTask"):
        self.project_id = lambda: project_id
        self.task_id = lambda: task_id


class TestExecutionContext(ExecutionContext):
    """dummy execution context that can be used in tests"""

    __test__ = False

    def __init__(self, project_id: str = "dummyProject", task_id: str = "dummyTask"):
        self.report = ReportContext()
        self.task = TestTaskContext(project_id=project_id, task_id=task_id)
        self.user = TestUserContext()


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

    @staticmethod
    def get_elements_len_from_stream(content: str) -> int:
        """Return elements len of xml file"""

        class MessageHandler(ContentHandler):
            """Message Handler"""

            def __init__(self):
                self.count = 0

            def startElement(self, name: str, attrs: AttributesImpl) -> None:  # noqa: N802
                _ = attrs

                if name == "Message":
                    self.count += 1

        handler = MessageHandler()
        parser = sax.make_parser()
        parser.setContentHandler(handler)
        parser.parse(content)
        return int(handler.count)
