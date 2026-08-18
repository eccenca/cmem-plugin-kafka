"""Testing utilities."""

import os
from http import HTTPStatus
from pathlib import Path
from typing import IO
from xml.sax.expatreader import AttributesImpl
from xml.sax.handler import ContentHandler

import httpx
import pytest

# check for cmem environment and skip if not present
from _pytest.mark import MarkDecorator
from cmem_client.client import Client
from cmem_client.models.dataset import Dataset, DatasetData, DatasetMetadata
from cmem_client.models.project import Project
from cmem_client.repositories.protocols.import_item import ImportConflictPolicy
from cmem_plugin_base.testing import TestExecutionContext, TestPluginContext, TestUserContext
from defusedxml import ElementTree, sax

from cmem_plugin_kafka.utils import get_dataset, get_resource_name

__all__ = [
    "FIXTURES_DIR",
    "TestExecutionContext",
    "TestPluginContext",
    "TestUserContext",
    "XMLUtils",
    "get_client",
    "get_kafka_config",
    "make_dataset",
    "make_project",
    "needs_cmem",
    "needs_kafka",
    "read_dataset_resource",
    "upload_resource",
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


def make_project(project_id: str) -> Client:
    """(Re-)create an empty project and provide a client for it"""
    client = get_client(project_id)
    client.projects.delete_item(project_id, skip_if_missing=True)
    client.projects.create_item(Project(name=project_id))
    return client


def make_dataset(
    client: Client, project_id: str, dataset_id: str, dataset_type: str, file_name: str
) -> None:
    """Create or replace a file based dataset in a project

    Replacing matters for projects which are imported from an archive and already
    bring the dataset with them.
    """
    dataset = Dataset(
        id=dataset_id,
        project=project_id,
        data=DatasetData(type=dataset_type, parameters={"file": file_name}),
        metadata=DatasetMetadata(label=dataset_id),
    )
    try:
        client.datasets.create_item(dataset)
    except httpx.HTTPStatusError as error:
        if error.response.status_code != HTTPStatus.CONFLICT:
            raise
        client.datasets.update_item(dataset)


def upload_resource(client: Client, project_id: str, file_name: str, path: Path) -> None:
    """Upload a local file as a project resource"""
    client.files.import_item(
        path=path,
        key=f"{project_id}:{file_name}",
        on_conflict=ImportConflictPolicy.REPLACE,
    )


def read_dataset_resource(project_id: str, dataset_id: str) -> bytes:
    """Read the file resource a dataset is based on into memory"""
    client = get_client(project_id)
    dataset = get_dataset(project_id=project_id, dataset_id=dataset_id, client=client)
    content: bytes = client.files.read(f"{project_id}:{get_resource_name(dataset)}")
    return content


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
    def get_message_count_from_stream(content: IO[bytes]) -> int:
        """Count the Message elements of an xml file without reading it as a whole"""

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
