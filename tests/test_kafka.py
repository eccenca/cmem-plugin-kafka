"""Plugin tests."""
import pytest
from cmem.cmempy.workspace.projects.datasets.dataset import make_new_dataset, get_dataset
from cmem.cmempy.workspace.projects.project import make_new_project, delete_project
from cmem.cmempy.workspace.projects.resources.resource import create_resource
from confluent_kafka import cimpl
from .utils import needs_cmem
from cmem_plugin_kafka.config import (
    BOOTSTRAP_SERVER,
    SECURITY_PROTOCOL,
    SASL_MECHANISMS,
    SASL_U,
    SASL_P,
    TOPIC
)
from cmem_plugin_kafka import KafkaPlugin

PROJECT_NAME = "kafka_test_project"
DATASET_NAME = "sample-test"
DATASET_TYPE = 'xml'
RESOURCE_NAME = f'{DATASET_NAME}.{DATASET_TYPE}'
DATASET_ID = f'{PROJECT_NAME}:{DATASET_NAME}'


@pytest.fixture
def setup(request):
    make_new_project(PROJECT_NAME)
    make_new_dataset(
        project_name=PROJECT_NAME,
        dataset_name=DATASET_NAME,
        dataset_type=DATASET_TYPE,
        parameters={"file": RESOURCE_NAME},
        autoconfigure=False,
    )
    with open('tests/sample-test.xml', 'rb') as response_file:
        create_resource(
            project_name=PROJECT_NAME,
            resource_name=RESOURCE_NAME,
            file_resource=response_file,
            replace=True,
        )

    def teardown():
        delete_project(PROJECT_NAME)

    request.addfinalizer(teardown)

    return get_dataset(PROJECT_NAME, DATASET_NAME)


@needs_cmem
def test_execution(setup):
    """Test plugin execution"""
    KafkaPlugin(
        message_dataset=DATASET_ID,
        bootstrap_servers=BOOTSTRAP_SERVER,
        security_protocol=SECURITY_PROTOCOL,
        sasl_mechanisms=SASL_MECHANISMS,
        sasl_username=SASL_U,
        sasl_password=SASL_P,
        kafka_topic=TOPIC
    ).execute()


def test_execution_plain_kafka(setup):
    """Test plugin execution for Plain Kafka"""
    KafkaPlugin(
        message_dataset=DATASET_ID,
        bootstrap_servers='172.17.0.1:9093',
        security_protocol='PLAINTEXT',
        sasl_mechanisms='PLAIN',
        sasl_username='',
        sasl_password='',
        kafka_topic=TOPIC
    ).execute()


@needs_cmem
def test_validate_invalid_inputs(setup):
    # Invalid Dataset
    with pytest.raises(ValueError):
        KafkaPlugin(
            message_dataset='sample',
            bootstrap_servers=BOOTSTRAP_SERVER,
            security_protocol=SECURITY_PROTOCOL,
            sasl_mechanisms=SASL_MECHANISMS,
            sasl_username=SASL_U,
            sasl_password=SASL_P,
            kafka_topic=TOPIC
        ).execute()

    # Invalid SECURITY PROTOCOL
    with pytest.raises(cimpl.KafkaException):
        KafkaPlugin(
            message_dataset=DATASET_ID,
            bootstrap_servers=BOOTSTRAP_SERVER,
            security_protocol='INVALID_PROTOCOL',
            sasl_mechanisms=SASL_MECHANISMS,
            sasl_username=SASL_U,
            sasl_password=SASL_P,
            kafka_topic=TOPIC
        ).execute()
