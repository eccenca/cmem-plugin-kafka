"""Plugin tests."""

import secrets
import string
from collections.abc import Generator
from contextlib import suppress
from pathlib import Path

import pytest
import requests
import xmltodict
from cmem.cmempy.workspace.projects.datasets.dataset import make_new_dataset
from cmem.cmempy.workspace.projects.project import delete_project, make_new_project
from cmem.cmempy.workspace.projects.resources.resource import create_resource
from cmem_plugin_examples.workflow.random_values import RandomValues
from confluent_kafka import KafkaException, cimpl

from cmem_plugin_kafka.utils import get_resource_from_dataset
from cmem_plugin_kafka.workflow.consumer import KafkaConsumerPlugin
from cmem_plugin_kafka.workflow.producer import KafkaProducerPlugin

from .utils import (
    TestExecutionContext,
    TestUserContext,
    get_kafka_config,
    needs_cmem,
    needs_kafka,
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


@pytest.fixture()
def project() -> Generator:
    """Provide the DI build project incl. assets."""
    with suppress(Exception):
        delete_project(PROJECT_NAME)
    make_new_project(PROJECT_NAME)
    make_new_dataset(
        project_name=PROJECT_NAME,
        dataset_name=PRODUCER_DATASET_NAME,
        dataset_type=DATASET_TYPE,
        parameters={"file": PRODUCER_RESOURCE_NAME},
        autoconfigure=False,
    )
    with Path("tests/sample-test.xml").open("rb") as response_file:
        create_resource(
            project_name=PROJECT_NAME,
            resource_name=PRODUCER_RESOURCE_NAME,
            file_resource=response_file,
            replace=True,
        )
    make_new_dataset(
        project_name=PROJECT_NAME,
        dataset_name=CONSUMER_DATASET_NAME,
        dataset_type=DATASET_TYPE,
        parameters={"file": CONSUMER_RESOURCE_NAME},
        autoconfigure=False,
    )
    yield PROJECT_NAME
    delete_project(PROJECT_NAME)


@needs_cmem
@needs_kafka
def test_execution_kafka_producer_new_topic(project: str) -> None:
    """Test producer with new topic"""
    # By default, new topic will not available
    with pytest.raises(
        ValueError,
        match=(
            "The topic you configured, was just created."
            " Save again if this ok for you."
            " Otherwise, change the topic name."
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
    resource, _ = get_resource_from_dataset(
        dataset_id=f"{project}:{CONSUMER_DATASET_NAME}", context=TestUserContext()
    )

    with Path("tests/sample-test.xml").open() as file:
        data = file.read().rstrip()
        data_dict = xmltodict.parse(data)
        messages = data_dict["KafkaMessages"]["Message"]
        for message in messages:
            if "@key" not in message:
                message["@key"] = ""
        assert xmltodict.parse(resource.text) == data_dict


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
    resource, _ = get_resource_from_dataset(
        dataset_id=f"{project}:{CONSUMER_DATASET_NAME}", context=TestUserContext()
    )

    with Path("tests/sample-test.xml").open() as file:
        data = file.read().rstrip()
        data_dict = xmltodict.parse(data)
        messages = data_dict["KafkaMessages"]["Message"]
        for message in messages:
            if "@key" not in message:
                message["@key"] = ""
        assert xmltodict.parse(resource.text) == data_dict


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
    resource, _ = get_resource_from_dataset(
        dataset_id=f"{PROJECT_NAME}:{CONSUMER_DATASET_NAME}", context=TestUserContext()
    )
    data_dict = xmltodict.parse(resource.text)
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
    resource, _ = get_resource_from_dataset(
        dataset_id=f"{project}:{CONSUMER_DATASET_NAME}", context=TestUserContext()
    )
    data_dict = xmltodict.parse(resource.text)
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
    resource, _ = get_resource_from_dataset(
        dataset_id=f"{PROJECT_NAME}:{CONSUMER_DATASET_NAME}", context=TestUserContext()
    )
    data_dict = xmltodict.parse(resource.text)
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
    resource, _ = get_resource_from_dataset(
        dataset_id=f"{project}:{CONSUMER_DATASET_NAME}", context=TestUserContext()
    )
    data_dict = xmltodict.parse(resource.text)
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
    with pytest.raises(requests.exceptions.HTTPError):
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
        match="KafkaError{code=_TRANSPORT,val=-195,"
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
        match="KafkaError{code=_AUTO_OFFSET_RESET,val=-140,"
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
