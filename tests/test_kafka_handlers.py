from contextlib import suppress
from dataclasses import dataclass

import ijson
import pytest
from cmem.cmempy.workspace.projects.datasets.dataset import make_new_dataset
from cmem.cmempy.workspace.projects.project import delete_project, make_new_project
from cmem.cmempy.workspace.projects.resources.resource import create_resource

from cmem_plugin_kafka.workflow.producer import KafkaProducerPlugin
from cmem_plugin_kafka.workflow.consumer import KafkaConsumerPlugin
from cmem_plugin_kafka.utils import get_resource_from_dataset
from .utils import get_kafka_config, needs_cmem, TestExecutionContext, TestUserContext
from confluent_kafka.admin import AdminClient, NewTopic

PROJECT_NAME = "kafka_test_project"
DATASET_NAME = "sample-test"
DATASET_TYPE = "json"
RESOURCE_NAME = f"{DATASET_NAME}.{DATASET_TYPE}"
DATASET_ID = f"{DATASET_NAME}"

KAFKA_CONFIG = get_kafka_config()
DEFAULT_TOPIC = "eccenca_kafka_handler_workflow"
DEFAULT_GROUP = "workflow"


@pytest.fixture
def topic(kafka_service):

    a = AdminClient({'bootstrap.servers': kafka_service})

    new_topics = [NewTopic(topic, num_partitions=1) for topic in [DEFAULT_TOPIC]]
    fs = a.create_topics(new_topics)

    # Wait for each operation to finish.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic {} created".format(topic))
        except Exception as e:
            print("Failed to create topic {}: {}".format(topic, e))
    yield DEFAULT_TOPIC
    fs = a.delete_topics([DEFAULT_TOPIC], operation_timeout=30)

    # Wait for operation to finish.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic {} deleted".format(topic))
        except Exception as e:
            print("Failed to delete topic {}: {}".format(topic, e))


@pytest.fixture
def project():
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
    with open("tests/sample-test.json", "rb") as response_file:
        create_resource(
            project_name=PROJECT_NAME,
            resource_name=RESOURCE_NAME,
            file_resource=response_file,
            replace=True,
        )

    @dataclass
    class FixtureData:
        """Class for providing fixture meta data."""
        project = PROJECT_NAME
        resource = RESOURCE_NAME
        dataset = DATASET_ID
    yield FixtureData()
    delete_project(PROJECT_NAME)


@needs_cmem
def test_kafka_json_data_handler(project, kafka_service, topic):
    """Validate KafkaJSONDataHandler"""
    _ = kafka_service
    KafkaProducerPlugin(
        message_dataset=project.dataset,
        bootstrap_servers=kafka_service,
        security_protocol="PLAINTEXT",
        sasl_mechanisms=None,
        sasl_username=None,
        sasl_password=None,
        kafka_topic=topic,
    ).execute(None, TestExecutionContext(project_id=project.project))
    # Consumer
    KafkaConsumerPlugin(
        message_dataset=project.dataset,
        bootstrap_servers=kafka_service,
        security_protocol="PLAINTEXT",
        sasl_mechanisms=None,
        sasl_username=None,
        sasl_password=None,
        kafka_topic=topic,
        group_id=DEFAULT_GROUP,
        auto_offset_reset="earliest",
    ).execute([], TestExecutionContext(project_id=project.project))

    # Ensure producer and consumer are working properly
    resource, _ = get_resource_from_dataset(
        dataset_id=f"{project.project}:{project.dataset}",
        context=TestUserContext(),
    )
    messages = ijson.items(resource.content, "item.message")
    count = 0
    for _ in messages:
        count = count + 1
    assert count == 2
