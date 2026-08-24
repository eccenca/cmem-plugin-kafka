"""Tests for producer/consumer plugin with big datasets."""

from collections.abc import Generator
from contextlib import suppress
from pathlib import Path

import httpx
import json_stream
import json_stream.httpx
import pytest
from cmem_client.repositories.protocols.import_item import ImportConflictPolicy
from cmem_plugin_examples.workflow.random_values import RandomValues

from cmem_plugin_kafka.utils import as_file_object, get_resource_from_dataset
from cmem_plugin_kafka.workflow.consumer import KafkaConsumerPlugin
from cmem_plugin_kafka.workflow.producer import KafkaProducerPlugin

from .utils import (
    TestExecutionContext,
    XMLUtils,
    get_client,
    get_kafka_config,
    make_dataset,
    make_project,
    needs_cmem,
    needs_kafka,
)

PROJECT_NAME = "kafka_performance_project"
PRODUCER_DATASET_NAME = "sample-test"
CONSUMER_DATASET_NAME = "sample-test-result"
DATASET_TYPE = "xml"
PRODUCER_RESOURCE_NAME = f"{PRODUCER_DATASET_NAME}.{DATASET_TYPE}"
CONSUMER_RESOURCE_NAME = f"{CONSUMER_DATASET_NAME}.{DATASET_TYPE}"
PRODUCER_DATASET_ID = f"{PRODUCER_DATASET_NAME}"
CONSUMER_DATASET_ID = f"{CONSUMER_DATASET_NAME}"

KAFKA_CONFIG = get_kafka_config()
DEFAULT_GROUP = ""
DEFAULT_TOPIC = "eccenca_kafka_workflow"
DEFAULT_RESET = "earliest"
XML_PROJECT_LINK = "https://download.eccenca.com/cmem-plugin-kafka/kafka_performance_project.zip"
JSON_PROJECT_LINK = "https://download.eccenca.com/cmem-plugin-kafka/kafka_json_perf_project.zip"


def download(url: str, path: Path) -> None:
    """Download a remote file to a local path"""
    with (
        httpx.stream("GET", url, timeout=10, follow_redirects=True) as response,
        path.open("wb") as local_file,
    ):
        response.raise_for_status()
        for chunk in response.iter_bytes():
            local_file.write(chunk)


def import_project(url: str, project_id: str, archive_name: str) -> None:
    """Download a project archive and import it, replacing an existing project"""
    archive = Path(archive_name)
    download(url, archive)
    client = get_client(project_id)
    client.projects.import_item(
        path=archive, key=project_id, on_conflict=ImportConflictPolicy.REPLACE
    )


@pytest.fixture
def xml_dataset_project() -> Generator:
    """Provide the DI build project incl. assets."""
    import_project(XML_PROJECT_LINK, PROJECT_NAME, "kafka_performance_project.zip")
    make_dataset(
        get_client(PROJECT_NAME),
        PROJECT_NAME,
        CONSUMER_DATASET_NAME,
        DATASET_TYPE,
        CONSUMER_RESOURCE_NAME,
    )
    yield PROJECT_NAME
    with suppress(Exception):
        Path("kafka_performance_project.zip").unlink()
        get_client(PROJECT_NAME).projects.delete_item(PROJECT_NAME)


@pytest.fixture
def entities_project() -> Generator:
    """Provide the DI build project incl. assets."""
    project_name = "kafka_entities_perf_project"
    make_project(project_name)
    yield project_name
    get_client(project_name).projects.delete_item(project_name)


@pytest.fixture
def json_dataset_project() -> Generator:
    """Provide the DI build project incl. assets."""
    project_name = "kafka_json_perf_project"
    import_project(JSON_PROJECT_LINK, project_name, "kafka_json_perf_project.zip")
    make_dataset(
        get_client(project_name),
        project_name,
        "json_dataset_result",
        "json",
        "json_dataset_result.json",
    )
    yield project_name
    with suppress(Exception):
        Path("kafka_json_perf_project.zip").unlink()
        get_client(project_name).projects.delete_item(project_name)


@needs_cmem
@needs_kafka
def test_perf_kafka_producer_consumer_xml_dataset(xml_dataset_project: str, topic: str) -> None:
    """Test plugin execution for Plain Kafka"""
    # Producer
    KafkaProducerPlugin(
        message_dataset=PRODUCER_DATASET_ID,
        bootstrap_servers=KAFKA_CONFIG["bootstrap_server"],
        security_protocol=KAFKA_CONFIG["security_protocol"],
        sasl_mechanisms=KAFKA_CONFIG["sasl_mechanisms"],
        sasl_username=KAFKA_CONFIG["sasl_username"],
        sasl_password=KAFKA_CONFIG["sasl_password"],
        kafka_topic=topic,
        client_id="",
    ).execute([], TestExecutionContext(project_id=xml_dataset_project))

    # Consumer
    KafkaConsumerPlugin(
        message_dataset=CONSUMER_DATASET_ID,
        bootstrap_servers=KAFKA_CONFIG["bootstrap_server"],
        security_protocol=KAFKA_CONFIG["security_protocol"],
        sasl_mechanisms=KAFKA_CONFIG["sasl_mechanisms"],
        sasl_username=KAFKA_CONFIG["sasl_username"],
        sasl_password=KAFKA_CONFIG["sasl_password"],
        kafka_topic=topic,
        group_id=DEFAULT_GROUP,
        auto_offset_reset=DEFAULT_RESET,
        message_limit=-1,
    ).execute([], TestExecutionContext(project_id=PROJECT_NAME))

    # Ensure producer and consumer are working properly
    resource, _ = get_resource_from_dataset(
        dataset_id=f"{PROJECT_NAME}:{CONSUMER_DATASET_NAME}",
        client=get_client(PROJECT_NAME),
    )
    with resource as consumer_file:
        consumer_file.raise_for_status()
        count = XMLUtils.get_message_count_from_stream(as_file_object(consumer_file))
        assert count == 286918  # noqa: PLR2004


@needs_cmem
@needs_kafka
def test_perf_kafka_producer_consumer_with_entities(entities_project: str, topic: str) -> None:
    """Test plugin execution for Plain Kafka"""
    no_of_entities = 1000000
    entities = RandomValues(
        random_function="token_urlsafe", number_of_entities=no_of_entities
    ).execute(context=TestExecutionContext())
    # Producer
    KafkaProducerPlugin(
        message_dataset="",
        bootstrap_servers=KAFKA_CONFIG["bootstrap_server"],
        security_protocol=KAFKA_CONFIG["security_protocol"],
        sasl_mechanisms=KAFKA_CONFIG["sasl_mechanisms"],
        sasl_username=KAFKA_CONFIG["sasl_username"],
        sasl_password=KAFKA_CONFIG["sasl_password"],
        kafka_topic=topic,
    ).execute([entities], TestExecutionContext(project_id=entities_project))

    # Consumer
    consumer_entities = KafkaConsumerPlugin(
        message_dataset="",
        bootstrap_servers=KAFKA_CONFIG["bootstrap_server"],
        security_protocol=KAFKA_CONFIG["security_protocol"],
        sasl_mechanisms=KAFKA_CONFIG["sasl_mechanisms"],
        sasl_username=KAFKA_CONFIG["sasl_username"],
        sasl_password=KAFKA_CONFIG["sasl_password"],
        kafka_topic=topic,
        group_id=DEFAULT_GROUP,
        auto_offset_reset="earliest",
        message_limit=-1,
    ).execute([], TestExecutionContext(project_id=entities_project))

    count = 0
    assert consumer_entities is not None
    assert (
        consumer_entities.schema.type_uri
        == "https://github.com/eccenca/cmem-plugin-kafka#PlainMessage"
    )
    assert len(consumer_entities.schema.paths) == 5  # noqa: PLR2004
    for _ in consumer_entities.entities:
        count += 1

    assert count == no_of_entities


@needs_cmem
@needs_kafka
def test_perf_kafka_producer_consumer_with_json_dataset(
    json_dataset_project: str, topic: str
) -> None:
    """Test plugin execution for Plain Kafka"""
    producer_dataset = "huge_json_dataset"
    consumer_dataset = "json_dataset_result"
    # Producer
    KafkaProducerPlugin(
        message_dataset=producer_dataset,
        bootstrap_servers=KAFKA_CONFIG["bootstrap_server"],
        security_protocol=KAFKA_CONFIG["security_protocol"],
        sasl_mechanisms=KAFKA_CONFIG["sasl_mechanisms"],
        sasl_username=KAFKA_CONFIG["sasl_username"],
        sasl_password=KAFKA_CONFIG["sasl_password"],
        kafka_topic=topic,
        client_id="",
    ).execute([], TestExecutionContext(project_id=json_dataset_project))
    # Consumer
    KafkaConsumerPlugin(
        message_dataset=consumer_dataset,
        bootstrap_servers=KAFKA_CONFIG["bootstrap_server"],
        security_protocol=KAFKA_CONFIG["security_protocol"],
        sasl_mechanisms=KAFKA_CONFIG["sasl_mechanisms"],
        sasl_username=KAFKA_CONFIG["sasl_username"],
        sasl_password=KAFKA_CONFIG["sasl_password"],
        kafka_topic=topic,
        group_id=DEFAULT_GROUP,
        auto_offset_reset=DEFAULT_RESET,
        message_limit=-1,
    ).execute([], TestExecutionContext(project_id=json_dataset_project))

    # Ensure producer and consumer are working properly
    resource, _ = get_resource_from_dataset(
        dataset_id=f"{json_dataset_project}:{consumer_dataset}",
        client=get_client(json_dataset_project),
    )
    with resource as json_file:
        json_file.raise_for_status()
        count = 0
        data = json_stream.httpx.load(json_file)
        for _ in data:
            count += 1
        assert count == 1000000  # noqa: PLR2004
