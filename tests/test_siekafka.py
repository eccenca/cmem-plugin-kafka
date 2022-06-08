"""Plugin tests."""
import pytest
import requests
from cmem.cmempy.workspace.projects.datasets.dataset import make_new_dataset, get_dataset
from cmem.cmempy.workspace.projects.project import make_new_project, delete_project
from cmem_plugin_base.dataintegration.parameter.dataset import DatasetParameterType
from confluent_kafka import cimpl
from .utils import needs_cmem
from cmem_plugin_siekafka.config import (
    BOOTSTRAP_SERVER,
    SECURITY_PROTOCOL,
    SASL_MECHANISMS,
    SASL_U,
    SASL_P,
    TOPIC
)
from cmem_plugin_siekafka import KafkaPlugin

PROJECT_NAME = "dateset_test_project"
DATASET_NAME = "sample_xml"
RESOURCE_NAME = "sample_xml.xml"
DATASET_ID = f'{PROJECT_NAME}:{DATASET_NAME}'


@pytest.fixture(scope="module")
def setup(request):
    make_new_project(PROJECT_NAME)
    make_new_dataset(project_name=PROJECT_NAME,
                     dataset_name=DATASET_NAME,
                     dataset_type="xml",
                     parameters={"file": RESOURCE_NAME},
                     autoconfigure=False
                     )

    def teardown():
        delete_project(PROJECT_NAME)

    request.addfinalizer(teardown)

    return get_dataset(PROJECT_NAME, DATASET_NAME)


@needs_cmem
def test_dataset_parameter_type_completion(setup):
    parameter = DatasetParameterType(dataset_type="xml")
    assert DATASET_ID in [x.value for x in parameter.autocomplete(query_terms=[])]
    assert len(parameter.autocomplete(query_terms=["lkshfkdsjfhsd"])) == 0


@needs_cmem
def test_execution(setup):
    """Test plugin execution"""
    KafkaPlugin(
        message_dataset='kafka-producer:kafka-messages',
        bootstrap_servers=BOOTSTRAP_SERVER,
        security_protocol=SECURITY_PROTOCOL,
        sasl_mechanisms=SASL_MECHANISMS,
        sasl_username=SASL_U,
        sasl_password=SASL_P,
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

    # Invalid Bootstrap Server
    with pytest.raises(requests.exceptions.HTTPError):
        KafkaPlugin(
            message_dataset=DATASET_ID,
            bootstrap_servers='INVALID_BOOTSTRAP_SERVER',
            security_protocol=SECURITY_PROTOCOL,
            sasl_mechanisms=SASL_MECHANISMS,
            sasl_username=SASL_U,
            sasl_password=SASL_P,
            kafka_topic=TOPIC
        ).execute()

    # Invalid SECURITY PROTOCOL
    with pytest.raises(cimpl.KafkaException, match=r'KafkaError{code=_INVALID_ARG,val=-186,str="Invalid value "(.*)" for configuration property "security.protocol""}'):
        KafkaPlugin(
            message_dataset=DATASET_ID,
            bootstrap_servers=BOOTSTRAP_SERVER,
            security_protocol='INVALID_PROTOCOL',
            sasl_mechanisms=SASL_MECHANISMS,
            sasl_username=SASL_U,
            sasl_password=SASL_P,
            kafka_topic=TOPIC
        ).execute()

    # Invalid SECURITY MECHANISMS
    with pytest.raises(cimpl.KafkaException, match=r'KafkaError{code=_INVALID_ARG,val=-186,str="Failed to create producer: Unsupported SASL mechanism:(.*)"}'):
        KafkaPlugin(
            message_dataset=DATASET_ID,
            bootstrap_servers=BOOTSTRAP_SERVER,
            security_protocol=SECURITY_PROTOCOL,
            sasl_mechanisms='INVALID_MECHANISMS',
            sasl_username=SASL_U,
            sasl_password=SASL_P,
            kafka_topic=TOPIC
        ).execute()
