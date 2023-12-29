"""Tests for `kafka_handlers` package."""
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path

import json_stream
import json_stream.requests
import pytest
from cmem.cmempy.workspace.projects.datasets.dataset import make_new_dataset
from cmem.cmempy.workspace.projects.project import delete_project, make_new_project
from cmem.cmempy.workspace.projects.resources.resource import create_resource

from cmem_plugin_kafka.utils import get_resource_from_dataset
from cmem_plugin_kafka.workflow.consumer import KafkaConsumerPlugin
from cmem_plugin_kafka.workflow.producer import KafkaProducerPlugin

from .utils import TestExecutionContext, TestUserContext, get_kafka_config, needs_cmem

PROJECT_NAME = "kafka_handler_test_project"
DATASET_NAME = "sample-test"
DATASET_TYPE = "json"
RESOURCE_NAME = f"{DATASET_NAME}.{DATASET_TYPE}"
DATASET_ID = f"{DATASET_NAME}"

KAFKA_CONFIG = get_kafka_config()
DEFAULT_TOPIC = "eccenca_kafka_handler_workflow"
DEFAULT_GROUP = "workflow"


@pytest.fixture()
def project() -> dataclass:
    """Provide the DI build project incl. assets."""
    with suppress(Exception):
        delete_project(PROJECT_NAME)
    make_new_project(PROJECT_NAME)
    make_new_dataset(
        project_name=PROJECT_NAME,
        dataset_name=DATASET_NAME,
        dataset_type=DATASET_TYPE,
        parameters={"file": RESOURCE_NAME},
        autoconfigure=False,
    )
    with Path("tests/sample-test.json").open("rb") as response_file:
        create_resource(
            project_name=PROJECT_NAME,
            resource_name=RESOURCE_NAME,
            file_resource=response_file,
            replace=True,
        )

    @dataclass
    class FixtureData:
        """Class for providing fixture meta data."""

        project = PROJECT_NAME
        resource = RESOURCE_NAME
        dataset = DATASET_ID

    yield FixtureData()
    delete_project(PROJECT_NAME)


@needs_cmem
def test_kafka_json_data_handler(project: dataclass, topic: str) -> None:
    """Validate KafkaJSONDataHandler"""
    kafka_service = KAFKA_CONFIG["bootstrap_server"]
    KafkaProducerPlugin(
        message_dataset=project.dataset,
        bootstrap_servers=kafka_service,
        security_protocol="PLAINTEXT",
        sasl_mechanisms=None,
        sasl_username=None,
        sasl_password=None,
        kafka_topic=topic,
    ).execute(None, TestExecutionContext(project_id=project.project))
    # Consumer
    KafkaConsumerPlugin(
        message_dataset=project.dataset,
        bootstrap_servers=kafka_service,
        security_protocol="PLAINTEXT",
        sasl_mechanisms=None,
        sasl_username=None,
        sasl_password=None,
        kafka_topic=topic,
        group_id=None,
        auto_offset_reset="earliest",
    ).execute([], TestExecutionContext(project_id=project.project))

    # Ensure producer and consumer are working properly
    resource, _ = get_resource_from_dataset(
        dataset_id=f"{project.project}:{project.dataset}",
        context=TestUserContext(),
    )
    assert len(resource.content) > 0, "JSON Content is empty"
    with Path("tests/sample-test.json").open("rb") as response_file:
        data = json_stream.to_standard_types(json_stream.load(response_file))
    with resource as consumer_dataset_file:
        consumer_data = json_stream.to_standard_types(
            json_stream.requests.load(consumer_dataset_file)
        )
    assert data == consumer_data
