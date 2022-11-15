"""Plugin tests."""
import zipfile
import random
import string
from contextlib import suppress
from datetime import time

import pytest
import requests
from cmem.cmempy.workspace.projects.datasets.dataset import make_new_dataset
from cmem.cmempy.workspace.projects.project import make_new_project, delete_project
from cmem.cmempy.workspace.projects.resources.resource import create_resource
from confluent_kafka import cimpl, KafkaException

from cmem_plugin_kafka.utils import get_resource_from_dataset
from cmem_plugin_kafka.workflow.consumer import KafkaConsumerPlugin
from cmem_plugin_kafka.workflow.producer import KafkaProducerPlugin
from cmem_plugin_kafka.utils import get_resource_from_dataset
from .utils import (
    needs_cmem,
    needs_kafka,
    get_kafka_config,
    XMLUtils,
    TestExecutionContext,
    TestUserContext,
)

PROJECT_NAME = "kafka_consumer_project"
PRODUCER_DATASET_NAME = "sample-test"
CONSUMER_DATASET_NAME = "sample-test-result"
DATASET_TYPE = "xml"
PRODUCER_RESOURCE_NAME = f"{PRODUCER_DATASET_NAME}.{DATASET_TYPE}"
CONSUMER_RESOURCE_NAME = f"{CONSUMER_DATASET_NAME}.{DATASET_TYPE}"
# DATASET_ID = f'{PROJECT_NAME}:{DATASET_NAME}'
PRODUCER_DATASET_ID = f"{PRODUCER_DATASET_NAME}"
CONSUMER_DATASET_ID = f"{CONSUMER_DATASET_NAME}"

KAFKA_CONFIG = get_kafka_config()
DEFAULT_GROUP = "workflow"
DEFAULT_TOPIC = "eccenca_kafka_workflow"
DEFAULT_RESET = "latest"


@pytest.fixture
def project(request):
    """Provides the DI build project incl. assets."""
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
    with open("tests/sample-test.xml", "rb") as response_file:
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
    request.addfinalizer(lambda: delete_project(PROJECT_NAME))


@needs_cmem
@needs_kafka
def test_execution_kafka_producer_consumer(project):
    """Test plugin execution for Plain Kafka"""
    with pytest.raises(ValueError, match="The topic you configured, was just created. Save again if this ok for you. "
                                         "Otherwise, change the topic name."):
        KafkaProducerPlugin(
            message_dataset=PRODUCER_DATASET_ID,
            bootstrap_servers=KAFKA_CONFIG["bootstrap_server"],
            security_protocol=KAFKA_CONFIG["security_protocol"],
            sasl_mechanisms=KAFKA_CONFIG["sasl_mechanisms"],
            sasl_username=KAFKA_CONFIG["sasl_username"],
            sasl_password=KAFKA_CONFIG["sasl_password"],
            kafka_topic=DEFAULT_TOPIC,
        ).execute([], TestExecutionContext(project_id=PROJECT_NAME))

    # Producer
    KafkaProducerPlugin(
        message_dataset=PRODUCER_DATASET_ID,
        bootstrap_servers=KAFKA_CONFIG["bootstrap_server"],
        security_protocol=KAFKA_CONFIG["security_protocol"],
        sasl_mechanisms=KAFKA_CONFIG["sasl_mechanisms"],
        sasl_username=KAFKA_CONFIG["sasl_username"],
        sasl_password=KAFKA_CONFIG["sasl_password"],
        kafka_topic=DEFAULT_TOPIC,
    ).execute([], TestExecutionContext(project_id=PROJECT_NAME))

    # Consumer
    KafkaConsumerPlugin(
        message_dataset=CONSUMER_DATASET_ID,
        bootstrap_servers=KAFKA_CONFIG["bootstrap_server"],
        security_protocol=KAFKA_CONFIG["security_protocol"],
        sasl_mechanisms=KAFKA_CONFIG["sasl_mechanisms"],
        sasl_username=KAFKA_CONFIG["sasl_username"],
        sasl_password=KAFKA_CONFIG["sasl_password"],
        kafka_topic=DEFAULT_TOPIC,
        group_id=DEFAULT_GROUP,
        auto_offset_reset="earliest",
    ).execute([], TestExecutionContext(project_id=PROJECT_NAME))

    # Ensure producer and consumer are working properly
    assert XMLUtils.get_elements_len_from_file(path="tests/sample-test.xml") == 3
    with get_resource_from_dataset(
        dataset_id=f"{PROJECT_NAME}:{CONSUMER_DATASET_NAME}", context=TestUserContext()
    ) as response:
        assert XMLUtils.get_elements_len_fromstring(
            response.text
        ) == XMLUtils.get_elements_len_from_file(path="tests/sample-test.xml")


@needs_cmem
@needs_kafka
def test_validate_invalid_inputs(project):
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
            kafka_topic=DEFAULT_TOPIC,
            group_id=DEFAULT_GROUP,
            auto_offset_reset=DEFAULT_RESET,
        ).execute([], TestExecutionContext(project_id=PROJECT_NAME))

    # Invalid SECURITY PROTOCOL
    with pytest.raises(cimpl.KafkaException):
        KafkaConsumerPlugin(
            message_dataset=CONSUMER_DATASET_ID,
            bootstrap_servers=KAFKA_CONFIG["bootstrap_server"],
            security_protocol="INVALID_PROTOCOL",
            sasl_mechanisms=KAFKA_CONFIG["sasl_mechanisms"],
            sasl_username=KAFKA_CONFIG["sasl_username"],
            sasl_password=KAFKA_CONFIG["sasl_password"],
            kafka_topic=DEFAULT_TOPIC,
            group_id=DEFAULT_GROUP,
            auto_offset_reset=DEFAULT_RESET,
        ).execute([], TestExecutionContext(project_id=PROJECT_NAME))


def test_validate_bootstrap_server():
    """Validate bootstrap service value"""
    with pytest.raises(ValueError, match="Specified server id is invalid"):
        KafkaConsumerPlugin(
            bootstrap_servers=1,
            message_dataset=CONSUMER_DATASET_ID,
            security_protocol=KAFKA_CONFIG["security_protocol"],
            sasl_mechanisms=KAFKA_CONFIG["sasl_mechanisms"],
            sasl_username=KAFKA_CONFIG["sasl_username"],
            sasl_password=KAFKA_CONFIG["sasl_password"],
            kafka_topic=DEFAULT_TOPIC,
            group_id=DEFAULT_GROUP,
            auto_offset_reset=DEFAULT_RESET,
        )

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
            kafka_topic=DEFAULT_TOPIC,
            group_id=DEFAULT_GROUP,
            auto_offset_reset=DEFAULT_RESET,
        )


@needs_cmem
@needs_kafka
def test_validate_auto_offset_reset_parameter(project):
    """Test plugin execution for Plain Kafka"""
    letters = string.ascii_letters
    NO_INITIAL_OFFSET_GROUP = (
        f"NO_INITIAL_OFFSET_GROUP_{''.join(random.choice(letters) for _ in range(10))}"
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
            kafka_topic=DEFAULT_TOPIC,
            group_id=NO_INITIAL_OFFSET_GROUP,
            auto_offset_reset="error",
        ).execute([], TestExecutionContext(project_id=PROJECT_NAME))
