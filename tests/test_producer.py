"""Plugin tests."""

from collections.abc import Generator
from pathlib import Path

import httpx
import pytest
from confluent_kafka import cimpl

from cmem_plugin_kafka.workflow.producer import KafkaProducerPlugin

from .utils import (
    FIXTURES_DIR,
    TestExecutionContext,
    get_client,
    get_kafka_config,
    make_dataset,
    make_project,
    needs_cmem,
    needs_kafka,
    upload_resource,
)

PROJECT_NAME = "kafka_test_project"
DATASET_NAME = "sample-test"
DATASET_TYPE = "xml"
RESOURCE_NAME = f"{DATASET_NAME}.{DATASET_TYPE}"
DATASET_ID = f"{DATASET_NAME}"

KAFKA_CONFIG = get_kafka_config()
DEFAULT_TOPIC = "eccenca_kafka_workflow"


@pytest.fixture
def project() -> Generator[str]:
    """Provide the DI build project incl. assets."""
    client = make_project(PROJECT_NAME)
    make_dataset(client, PROJECT_NAME, DATASET_NAME, DATASET_TYPE, RESOURCE_NAME)
    upload_resource(client, PROJECT_NAME, RESOURCE_NAME, Path(FIXTURES_DIR / "sample-test.xml"))

    yield PROJECT_NAME
    get_client(PROJECT_NAME).projects.delete_item(PROJECT_NAME)


@needs_cmem
@needs_kafka
def test_execution_plain_kafka(project: str, topic: str) -> None:
    """Test plugin execution for Plain Kafka"""
    KafkaProducerPlugin(
        message_dataset=DATASET_ID,
        bootstrap_servers=KAFKA_CONFIG["bootstrap_server"],
        security_protocol=KAFKA_CONFIG["security_protocol"],
        sasl_mechanisms=KAFKA_CONFIG["sasl_mechanisms"],
        sasl_username=KAFKA_CONFIG["sasl_username"],
        sasl_password=KAFKA_CONFIG["sasl_password"],
        kafka_topic=topic,
    ).execute([], TestExecutionContext(project_id=project))


@needs_cmem
@needs_kafka
def test_validate_invalid_inputs(project: str, topic: str) -> None:
    """Test producer plugin validation with invalid inputs"""
    # Invalid Dataset
    with pytest.raises(httpx.HTTPStatusError):
        KafkaProducerPlugin(
            message_dataset="sample",
            bootstrap_servers=KAFKA_CONFIG["bootstrap_server"],
            security_protocol=KAFKA_CONFIG["security_protocol"],
            sasl_mechanisms=KAFKA_CONFIG["sasl_mechanisms"],
            sasl_username=KAFKA_CONFIG["sasl_username"],
            sasl_password=KAFKA_CONFIG["sasl_password"],
            kafka_topic=topic,
        ).execute([], TestExecutionContext(project_id=project))

    # Invalid SECURITY PROTOCOL
    with pytest.raises(cimpl.KafkaException):
        KafkaProducerPlugin(
            message_dataset=DATASET_ID,
            bootstrap_servers=KAFKA_CONFIG["bootstrap_server"],
            security_protocol="INVALID_PROTOCOL",
            sasl_mechanisms=KAFKA_CONFIG["sasl_mechanisms"],
            sasl_username=KAFKA_CONFIG["sasl_username"],
            sasl_password=KAFKA_CONFIG["sasl_password"],
            kafka_topic=DEFAULT_TOPIC,
        ).execute([], TestExecutionContext(project_id=PROJECT_NAME))


def test_validate_bootstrap_server() -> None:
    """Validate bootstrap service value"""
    with pytest.raises(
        cimpl.KafkaException,
        match=r"KafkaError{code=_TRANSPORT,val=-195,"
        'str="Failed to get metadata: Local: Broker transport failure"}',
    ):
        KafkaProducerPlugin(
            bootstrap_servers="invalid_bootstrap_server:9092",
            message_dataset=DATASET_ID,
            security_protocol="PLAINTEXT",
            sasl_mechanisms="PLAIN",
            sasl_username="",
            sasl_password="",
            kafka_topic=DEFAULT_TOPIC,
        ).execute([], TestExecutionContext(project_id=PROJECT_NAME))
