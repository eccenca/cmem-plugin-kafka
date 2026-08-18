"""Plugin tests."""

import secrets
import string
from collections.abc import Generator
from pathlib import Path

import httpx
import pytest
import xmltodict
from cmem_plugin_examples.workflow.random_values import RandomValues
from confluent_kafka import KafkaException, cimpl

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
    needs_kafka,
    read_dataset_resource,
    upload_resource,
)

PROJECT_NAME = "kafka_consumer_project"
PRODUCER_DATASET_NAME = "sample-test"
CONSUMER_DATASET_NAME = "sample-test-result"
DATASET_TYPE = "xml"
PRODUCER_RESOURCE_NAME = f"{PRODUCER_DATASET_NAME}.{DATASET_TYPE}"
CONSUMER_RESOURCE_NAME = f"{CONSUMER_DATASET_NAME}.{DATASET_TYPE}"
PRODUCER_DATASET_ID = f"{PRODUCER_DATASET_NAME}"
CONSUMER_DATASET_ID = f"{CONSUMER_DATASET_NAME}"

KAFKA_CONFIG = get_kafka_config()
DEFAULT_GROUP = ""
DEFAULT_RESET = "latest"


@pytest.fixture
def project() -> Generator:
    """Provide the DI build project incl. assets."""
    client = make_project(PROJECT_NAME)
    make_dataset(client, PROJECT_NAME, PRODUCER_DATASET_NAME, DATASET_TYPE, PRODUCER_RESOURCE_NAME)
    upload_resource(
        client, PROJECT_NAME, PRODUCER_RESOURCE_NAME, Path(FIXTURES_DIR / "sample-test.xml")
    )
    make_dataset(client, PROJECT_NAME, CONSUMER_DATASET_NAME, DATASET_TYPE, CONSUMER_RESOURCE_NAME)
    yield PROJECT_NAME
    get_client(PROJECT_NAME).projects.delete_item(PROJECT_NAME)


@needs_cmem
@needs_kafka
def test_execution_kafka_producer_new_topic(project: str) -> None:
    """Test producer with new topic"""
    # By default, new topic will not available
    with pytest.raises(
        ValueError,
        match=(
            r"The topic you configured, was just created."
            r" Save again if this ok for you."
            r" Otherwise, change the topic name."
        ),
    ):
        KafkaProducerPlugin(
            message_dataset=PRODUCER_DATASET_ID,
            bootstrap_servers=KAFKA_CONFIG["bootstrap_server"],
            security_protocol=KAFKA_CONFIG["security_protocol"],
            sasl_mechanisms=KAFKA_CONFIG["sasl_mechanisms"],
            sasl_username=KAFKA_CONFIG["sasl_username"],
            sasl_password=KAFKA_CONFIG["sasl_password"],
            kafka_topic="NEW_TOPIC_" + str(secrets.randbelow(100)),
        ).execute([], TestExecutionContext(project_id=project))


@needs_cmem
@needs_kafka
def test_execution_kafka_producer_consumer_with_xml_dataset(project: str, topic: str) -> None:
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
    ).execute([], TestExecutionContext(project_id=project))

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
        auto_offset_reset="earliest",
    ).execute([], TestExecutionContext(project_id=project))

    # Ensure producer and consumer are working properly
    resource = read_dataset_resource(project, CONSUMER_DATASET_NAME)

    with Path(FIXTURES_DIR / "sample-test.xml").open() as file:
        data = file.read().rstrip()
        data_dict = xmltodict.parse(data)
        messages = data_dict["KafkaMessages"]["Message"]
        for message in messages:
            if "@key" not in message:
                message["@key"] = ""
        assert xmltodict.parse(resource) == data_dict


@needs_cmem
@needs_kafka
@pytest.mark.parametrize("compression_type", ["gzip", "snappy", "lz4", "zstd"])
def test_validate_compression(project: str, topic: str, compression_type: str) -> None:
    """Test to validate compression type"""
    # Producer
    KafkaProducerPlugin(
        message_dataset=PRODUCER_DATASET_ID,
        bootstrap_servers=KAFKA_CONFIG["bootstrap_server"],
        security_protocol=KAFKA_CONFIG["security_protocol"],
        sasl_mechanisms=KAFKA_CONFIG["sasl_mechanisms"],
        sasl_username=KAFKA_CONFIG["sasl_username"],
        sasl_password=KAFKA_CONFIG["sasl_password"],
        kafka_topic=topic,
        compression_type=compression_type,
    ).execute([], TestExecutionContext(project_id=project))

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
        auto_offset_reset="earliest",
    ).execute([], TestExecutionContext(project_id=project))

    # Ensure producer and consumer are working properly
    resource = read_dataset_resource(project, CONSUMER_DATASET_NAME)

    with Path(FIXTURES_DIR / "sample-test.xml").open() as file:
        data = file.read().rstrip()
        data_dict = xmltodict.parse(data)
        messages = data_dict["KafkaMessages"]["Message"]
        for message in messages:
            if "@key" not in message:
                message["@key"] = ""
        assert xmltodict.parse(resource) == data_dict


@needs_cmem
@needs_kafka
def test_validate_message_limit_parameter(project: str, topic: str) -> None:
    """Test to validate message limit"""
    # Producer
    KafkaProducerPlugin(
        message_dataset=PRODUCER_DATASET_ID,
        bootstrap_servers=KAFKA_CONFIG["bootstrap_server"],
        security_protocol=KAFKA_CONFIG["security_protocol"],
        sasl_mechanisms=KAFKA_CONFIG["sasl_mechanisms"],
        sasl_username=KAFKA_CONFIG["sasl_username"],
        sasl_password=KAFKA_CONFIG["sasl_password"],
        kafka_topic=topic,
    ).execute([], TestExecutionContext(project_id=project))

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
        auto_offset_reset="earliest",
        message_limit=2,
    ).execute([], TestExecutionContext(project_id=project))

    # Ensure producer and consumer are working properly
    resource = read_dataset_resource(PROJECT_NAME, CONSUMER_DATASET_NAME)
    data_dict = xmltodict.parse(resource)
    assert len(data_dict["KafkaMessages"]["Message"]) == 2  # noqa: PLR2004


@needs_cmem
@needs_kafka
def test_validate_disable_commit_parameter(project: str, topic: str) -> None:
    """Test to validate with disable commit parameter"""
    # Producer
    KafkaProducerPlugin(
        message_dataset=PRODUCER_DATASET_ID,
        bootstrap_servers=KAFKA_CONFIG["bootstrap_server"],
        security_protocol=KAFKA_CONFIG["security_protocol"],
        sasl_mechanisms=KAFKA_CONFIG["sasl_mechanisms"],
        sasl_username=KAFKA_CONFIG["sasl_username"],
        sasl_password=KAFKA_CONFIG["sasl_password"],
        kafka_topic=topic,
    ).execute([], TestExecutionContext(project_id=project))

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
        auto_offset_reset="earliest",
        disable_commit=True,
    ).execute([], TestExecutionContext(project_id=project))

    # Ensure producer and consumer are working properly
    resource = read_dataset_resource(project, CONSUMER_DATASET_NAME)
    data_dict = xmltodict.parse(resource)
    assert len(data_dict["KafkaMessages"]["Message"]) == 3  # noqa: PLR2004

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
        auto_offset_reset="earliest",
        disable_commit=False,
    ).execute([], TestExecutionContext(project_id=project))

    # Ensure producer and consumer are working properly
    resource = read_dataset_resource(PROJECT_NAME, CONSUMER_DATASET_NAME)
    data_dict = xmltodict.parse(resource)
    assert len(data_dict["KafkaMessages"]["Message"]) == 3  # noqa: PLR2004
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
        auto_offset_reset="earliest",
        disable_commit=False,
    ).execute([], TestExecutionContext(project_id=project))

    # Ensure producer and consumer are working properly
    resource = read_dataset_resource(project, CONSUMER_DATASET_NAME)
    data_dict = xmltodict.parse(resource)
    assert not data_dict["KafkaMessages"]


@needs_cmem
@needs_kafka
def test_execution_kafka_producer_consumer_with_entities(project: str, topic: str) -> None:
    """Test plugin execution for Plain Kafka"""
    entities = RandomValues(random_function="token_urlsafe").execute(context=TestExecutionContext())
    # Producer
    KafkaProducerPlugin(
        message_dataset="",
        bootstrap_servers=KAFKA_CONFIG["bootstrap_server"],
        security_protocol=KAFKA_CONFIG["security_protocol"],
        sasl_mechanisms=KAFKA_CONFIG["sasl_mechanisms"],
        sasl_username=KAFKA_CONFIG["sasl_username"],
        sasl_password=KAFKA_CONFIG["sasl_password"],
        kafka_topic=topic,
    ).execute([entities], TestExecutionContext(project_id=project))

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
    ).execute([], TestExecutionContext(project_id=project))
    count = 0
    assert consumer_entities is not None
    assert (
        consumer_entities.schema.type_uri
        == "https://github.com/eccenca/cmem-plugin-kafka#PlainMessage"
    )
    assert len(consumer_entities.schema.paths) == 5  # noqa: PLR2004
    for _ in consumer_entities.entities:
        count += 1

    assert count == 10  # noqa: PLR2004


@needs_cmem
@needs_kafka
def test_validate_invalid_inputs(project: str, topic: str) -> None:
    """Validate Invalid Inputs"""
    # Invalid Dataset
    with pytest.raises(httpx.HTTPStatusError):
        KafkaConsumerPlugin(
            message_dataset="sample",
            bootstrap_servers=KAFKA_CONFIG["bootstrap_server"],
            security_protocol=KAFKA_CONFIG["security_protocol"],
            sasl_mechanisms=KAFKA_CONFIG["sasl_mechanisms"],
            sasl_username=KAFKA_CONFIG["sasl_username"],
            sasl_password=KAFKA_CONFIG["sasl_password"],
            kafka_topic=topic,
            group_id=DEFAULT_GROUP,
            auto_offset_reset=DEFAULT_RESET,
        ).execute([], TestExecutionContext(project_id=project))

    # Invalid SECURITY PROTOCOL
    with pytest.raises(cimpl.KafkaException):
        KafkaConsumerPlugin(
            message_dataset=CONSUMER_DATASET_ID,
            bootstrap_servers=KAFKA_CONFIG["bootstrap_server"],
            security_protocol="INVALID_PROTOCOL",
            sasl_mechanisms=KAFKA_CONFIG["sasl_mechanisms"],
            sasl_username=KAFKA_CONFIG["sasl_username"],
            sasl_password=KAFKA_CONFIG["sasl_password"],
            kafka_topic=topic,
            group_id=DEFAULT_GROUP,
            auto_offset_reset=DEFAULT_RESET,
        ).execute([], TestExecutionContext(project_id=project))


def test_validate_bootstrap_server() -> None:
    """Validate bootstrap service value"""
    with pytest.raises(
        cimpl.KafkaException,
        match=r"KafkaError{code=_TRANSPORT,val=-195,"
        'str="Failed to get metadata: Local: Broker transport failure"}',
    ):
        KafkaConsumerPlugin(
            bootstrap_servers="invalid_bootstrap_server:9092",
            message_dataset=CONSUMER_DATASET_ID,
            security_protocol="PLAINTEXT",
            sasl_mechanisms="PLAIN",
            sasl_username="",
            sasl_password="",
            kafka_topic="DEFAULT_TOPIC",
            group_id=DEFAULT_GROUP,
            auto_offset_reset=DEFAULT_RESET,
        ).execute([], TestExecutionContext(project_id=PROJECT_NAME))


@needs_cmem
@needs_kafka
def test_validate_auto_offset_reset_parameter(project: str, topic: str) -> None:
    """Test plugin execution for Plain Kafka"""
    letters = string.ascii_letters
    no_initial_offset_group = (
        f"NO_INITIAL_OFFSET_GROUP_{''.join(secrets.choice(letters) for _ in range(10))}"
    )

    with pytest.raises(
        KafkaException,
        match=r"KafkaError{code=_AUTO_OFFSET_RESET,val=-140,"
        'str="no previously committed offset available: Local: No offset stored"}',
    ):
        KafkaConsumerPlugin(
            message_dataset=CONSUMER_DATASET_ID,
            bootstrap_servers=KAFKA_CONFIG["bootstrap_server"],
            security_protocol=KAFKA_CONFIG["security_protocol"],
            sasl_mechanisms=KAFKA_CONFIG["sasl_mechanisms"],
            sasl_username=KAFKA_CONFIG["sasl_username"],
            sasl_password=KAFKA_CONFIG["sasl_password"],
            kafka_topic=topic,
            group_id=no_initial_offset_group,
            auto_offset_reset="error",
        ).execute([], TestExecutionContext(project_id=project))
