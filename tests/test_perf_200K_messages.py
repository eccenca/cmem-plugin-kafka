import os
import shutil
from contextlib import suppress

import pytest
import requests
from cmem.cmempy.workspace.projects.datasets.dataset import make_new_dataset
from cmem.cmempy.workspace.projects.import_ import (
    upload_project,
    import_from_upload_start,
    import_from_upload_status,
)
from cmem.cmempy.workspace.projects.project import delete_project

from cmem_plugin_kafka.utils import get_resource_from_dataset
from cmem_plugin_kafka.workflow.consumer import KafkaConsumerPlugin
from cmem_plugin_kafka.workflow.producer import KafkaProducerPlugin
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
DEFAULT_GROUP = None
DEFAULT_TOPIC = "eccenca_kafka_workflow"
DEFAULT_RESET = "earliest"
RESOURCE_LINK = (
    "https://download.eccenca.com/cmem-plugin-kafka/kafka_performance_project.zip"
)


@pytest.fixture
def perf_project(request):
    """Provides the DI build project incl. assets."""
    with suppress(Exception):
        delete_project(PROJECT_NAME)

    with requests.get(url=RESOURCE_LINK, timeout=10, stream=True) as response:
        with open("kafka_performance_project.zip", "wb") as project_file:
            shutil.copyfileobj(response.raw, project_file)

    validation_response = upload_project("kafka_performance_project.zip")
    import_id = validation_response["projectImportId"]
    project_id = validation_response["projectId"]

    import_from_upload_start(
        import_id=import_id, project_id=project_id, overwrite_existing=True
    )
    # loop until "success" boolean is in status response
    status = import_from_upload_status(import_id)
    while "success" not in status.keys():
        status = import_from_upload_status(import_id)

    make_new_dataset(
        project_name=PROJECT_NAME,
        dataset_name=CONSUMER_DATASET_NAME,
        dataset_type=DATASET_TYPE,
        parameters={"file": CONSUMER_RESOURCE_NAME},
        autoconfigure=False,
    )
    yield request
    with suppress(Exception):
        os.remove("kafka_performance_project.zip")
        delete_project(PROJECT_NAME)


@needs_cmem
@needs_kafka
def test_performance_execution_kafka_producer(perf_project, topic):
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
    ).execute([], TestExecutionContext(project_id=PROJECT_NAME))

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
    ).execute([], TestExecutionContext(project_id=PROJECT_NAME))

    # Ensure producer and consumer are working properly
    resource, _ = get_resource_from_dataset(
        dataset_id=f"{PROJECT_NAME}:{CONSUMER_DATASET_NAME}",
        context=TestUserContext(),
    )
    with resource as consumer_file:
        consumer_file.raw.decode_content = True
        assert XMLUtils.get_elements_len_from_stream(consumer_file.raw) == 286918
