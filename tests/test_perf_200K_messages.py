from contextlib import suppress
import requests

import pytest
from cmem.cmempy.workspace.projects.datasets.dataset import make_new_dataset
from cmem.cmempy.workspace.projects.project import make_new_project, delete_project
from cmem.cmempy.workspace.projects.resources.resource import create_resource

from cmem_plugin_kafka.workflow.producer import KafkaProducerPlugin
from cmem_plugin_kafka.utils import get_resource_from_dataset
from cmem_plugin_kafka.workflow.consumer import KafkaConsumerPlugin
from .utils import (
    needs_cmem,
    needs_kafka,
    get_kafka_config,
    TestExecutionContext,
    XMLUtils,
    TestUserContext,
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
DEFAULT_GROUP = "workflow"
DEFAULT_TOPIC = "eccenca_kafka_workflow"
DEFAULT_RESET = "latest"
RESOURCE_LINK = "https://download.eccenca.com/cmem-plugin-kafka/286K_Message.zip"
RESOURCE_FILE = "tests/200K-Messages.xml"

@pytest.fixture
def perf_project(request):
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
    response = requests.get(url=RESOURCE_LINK, verify=False, timeout=10)
    with open(RESOURCE_FILE, "wb") as file:
        file.write(response.content)
    with open(RESOURCE_FILE, "rb") as response_file:
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
        parameters={"file": PRODUCER_RESOURCE_NAME},
        autoconfigure=False,
    )
    yield request
    with suppress(Exception):
        delete_project(PROJECT_NAME)


@needs_cmem
@needs_kafka
def test_performance_execution_kafka_producer_consumer(perf_project):
    """Test plugin execution for Plain Kafka"""
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
    with get_resource_from_dataset(
        dataset_id=f"{PROJECT_NAME}:{PRODUCER_DATASET_NAME}", context=TestUserContext()
    ) as producer_file:
        with get_resource_from_dataset(
            dataset_id=f"{PROJECT_NAME}:{CONSUMER_DATASET_NAME}",
            context=TestUserContext(),
        ) as consumer_file:
            assert XMLUtils.get_elements_len_fromstring(
                consumer_file.text
            ) == XMLUtils.get_elements_len_fromstring(producer_file.text)
