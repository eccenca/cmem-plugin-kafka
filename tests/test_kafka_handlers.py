"""Tests for `kafka_handlers` package."""

from dataclasses import dataclass
from io import BytesIO
from pathlib import Path

import json_stream
import pytest

from cmem_plugin_kafka.workflow.consumer import KafkaConsumerPlugin
from cmem_plugin_kafka.workflow.producer import KafkaProducerPlugin

from .utils import (
    FIXTURES_DIR,
    TestExecutionContext,
    get_client,
    get_kafka_config,
    make_dataset,
    make_project,
    needs_cmem,
    read_dataset_resource,
    upload_resource,
)

PROJECT_NAME = "kafka_handler_test_project"
DATASET_NAME = "sample-test"
DATASET_TYPE = "json"
RESOURCE_NAME = f"{DATASET_NAME}.{DATASET_TYPE}"
DATASET_ID = f"{DATASET_NAME}"

KAFKA_CONFIG = get_kafka_config()
DEFAULT_TOPIC = "eccenca_kafka_handler_workflow"
DEFAULT_GROUP = "workflow"


@pytest.fixture
def project():  # noqa: ANN201
    """Provide the DI build project incl. assets."""
    client = make_project(PROJECT_NAME)
    make_dataset(client, PROJECT_NAME, DATASET_NAME, DATASET_TYPE, RESOURCE_NAME)
    upload_resource(client, PROJECT_NAME, RESOURCE_NAME, Path(FIXTURES_DIR / "sample-test.json"))

    @dataclass
    class FixtureData:
        """Class for providing fixture meta data."""

        project = PROJECT_NAME
        resource = RESOURCE_NAME
        dataset = DATASET_ID

    yield FixtureData()
    get_client(PROJECT_NAME).projects.delete_item(PROJECT_NAME)


@needs_cmem
def test_kafka_json_data_handler(project, topic: str) -> None:  # noqa: ANN001
    """Validate KafkaJSONDataHandler"""
    kafka_service = KAFKA_CONFIG["bootstrap_server"]
    KafkaProducerPlugin(
        message_dataset=project.dataset,
        bootstrap_servers=kafka_service,
        security_protocol="PLAINTEXT",
        sasl_mechanisms="",
        sasl_username="",
        sasl_password="",
        kafka_topic=topic,
    ).execute([], TestExecutionContext(project_id=project.project))
    # Consumer
    KafkaConsumerPlugin(
        message_dataset=project.dataset,
        bootstrap_servers=kafka_service,
        security_protocol="PLAINTEXT",
        sasl_mechanisms="",
        sasl_username="",
        sasl_password="",
        kafka_topic=topic,
        group_id="",
        auto_offset_reset="earliest",
    ).execute([], TestExecutionContext(project_id=project.project))

    # Ensure producer and consumer are working properly
    resource = read_dataset_resource(project.project, project.dataset)
    assert len(resource) > 0, "JSON Content is empty"
    with Path(FIXTURES_DIR / "sample-test.json").open("rb") as response_file:
        data = json_stream.to_standard_types(json_stream.load(response_file))
    consumer_data = json_stream.to_standard_types(json_stream.load(BytesIO(resource)))
    assert data == consumer_data
