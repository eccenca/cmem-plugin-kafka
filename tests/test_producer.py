"""Plugin tests."""
from contextlib import suppress

import pytest
import requests
from cmem.cmempy.workspace.projects.datasets.dataset import make_new_dataset
from cmem.cmempy.workspace.projects.project import make_new_project, delete_project
from cmem.cmempy.workspace.projects.resources.resource import create_resource
from confluent_kafka import cimpl

from cmem_plugin_kafka.workflow.producer import KafkaProducerPlugin
from .utils import needs_cmem, needs_kafka, get_kafka_config, TestExecutionContext

PROJECT_NAME = "kafka_test_project"
DATASET_NAME = "sample-test"
DATASET_TYPE = "xml"
RESOURCE_NAME = f"{DATASET_NAME}.{DATASET_TYPE}"
DATASET_ID = f"{DATASET_NAME}"

KAFKA_CONFIG = get_kafka_config()
DEFAULT_TOPIC = "eccenca_kafka_workflow"


@pytest.fixture
def project(request):
    """Provides the DI build project incl. assets."""
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
    with open("tests/sample-test.xml", "rb") as response_file:
        create_resource(
            project_name=PROJECT_NAME,
            resource_name=RESOURCE_NAME,
            file_resource=response_file,
            replace=True,
        )
    request.addfinalizer(lambda: delete_project(PROJECT_NAME))


@needs_cmem
@needs_kafka
def test_execution_plain_kafka(project):
    """Test plugin execution for Plain Kafka"""
    KafkaProducerPlugin(
        message_dataset=DATASET_ID,
        bootstrap_servers=KAFKA_CONFIG["bootstrap_server"],
        security_protocol=KAFKA_CONFIG["security_protocol"],
        sasl_mechanisms=KAFKA_CONFIG["sasl_mechanisms"],
        sasl_username=KAFKA_CONFIG["sasl_username"],
        sasl_password=KAFKA_CONFIG["sasl_password"],
        kafka_topic=DEFAULT_TOPIC,
    ).execute(None, TestExecutionContext(project_id=PROJECT_NAME))


@needs_cmem
@needs_kafka
def test_validate_invalid_inputs(project):
    # Invalid Dataset
    with pytest.raises(requests.exceptions.HTTPError):
        KafkaProducerPlugin(
            message_dataset="sample",
            bootstrap_servers=KAFKA_CONFIG["bootstrap_server"],
            security_protocol=KAFKA_CONFIG["security_protocol"],
            sasl_mechanisms=KAFKA_CONFIG["sasl_mechanisms"],
            sasl_username=KAFKA_CONFIG["sasl_username"],
            sasl_password=KAFKA_CONFIG["sasl_password"],
            kafka_topic=DEFAULT_TOPIC,
        ).execute(None, TestExecutionContext(project_id=PROJECT_NAME))

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
        ).execute(None, TestExecutionContext(project_id=PROJECT_NAME))


def test_validate_bootstrap_server():
    """Validate bootstrap service value"""
    with pytest.raises(ValueError, match="Specified server id is invalid"):
        KafkaProducerPlugin(
            bootstrap_servers=1,
            message_dataset=DATASET_ID,
            security_protocol=KAFKA_CONFIG["security_protocol"],
            sasl_mechanisms=KAFKA_CONFIG["sasl_mechanisms"],
            sasl_username=KAFKA_CONFIG["sasl_username"],
            sasl_password=KAFKA_CONFIG["sasl_password"],
            kafka_topic=DEFAULT_TOPIC,
        )

    with pytest.raises(
        cimpl.KafkaException,
        match="KafkaError{code=_TRANSPORT,val=-195,"
        'str="Failed to get metadata: Local: Broker transport failure"}',
    ):
        KafkaProducerPlugin(
            bootstrap_servers="invalid_bootstrap_server:9092",
            message_dataset=DATASET_ID,
            security_protocol="PLAINTEXT",
            sasl_mechanisms="PLAIN",
            sasl_username=None,
            sasl_password=None,
            kafka_topic=DEFAULT_TOPIC,
        ).execute(None, None)
