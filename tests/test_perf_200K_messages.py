import os
import shutil
from contextlib import suppress

import json_stream.requests
import pytest
import requests
from cmem.cmempy.workspace.projects.datasets.dataset import make_new_dataset
from cmem.cmempy.workspace.projects.import_ import (
    import_from_upload_start,
    import_from_upload_status,
    upload_project,
)
from cmem.cmempy.workspace.projects.project import delete_project, make_new_project
from cmem_plugin_base.dataintegration.utils import setup_cmempy_user_access
from cmem_plugin_examples.workflow.random_values import RandomValues

from cmem_plugin_kafka.utils import get_resource_from_dataset
from cmem_plugin_kafka.workflow.consumer import KafkaConsumerPlugin
from cmem_plugin_kafka.workflow.producer import KafkaProducerPlugin

from .utils import (
    TestExecutionContext,
    TestUserContext,
    XMLUtils,
    get_kafka_config,
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
DEFAULT_GROUP = None
DEFAULT_TOPIC = "eccenca_kafka_workflow"
DEFAULT_RESET = "earliest"
XML_PROJECT_LINK = "https://download.eccenca.com/cmem-plugin-kafka/kafka_performance_project.zip"
JSON_PROJECT_LINK = "https://download.eccenca.com/cmem-plugin-kafka/kafka_json_perf_project.zip"


@pytest.fixture()
def xml_dataset_project():
    """Provides the DI build project incl. assets."""
    setup_cmempy_user_access(context=TestUserContext())
    with suppress(Exception):
        delete_project(PROJECT_NAME)

    with requests.get(url=XML_PROJECT_LINK, timeout=10, stream=True) as response:
        with open("kafka_performance_project.zip", "wb") as project_file:
            shutil.copyfileobj(response.raw, project_file)

    validation_response = upload_project("kafka_performance_project.zip")
    import_id = validation_response["projectImportId"]
    project_id = validation_response["projectId"]

    import_from_upload_start(import_id=import_id, project_id=project_id, overwrite_existing=True)
    # loop until "success" boolean is in status response
    status = import_from_upload_status(import_id)
    while "success" not in status:
        status = import_from_upload_status(import_id)

    make_new_dataset(
        project_name=PROJECT_NAME,
        dataset_name=CONSUMER_DATASET_NAME,
        dataset_type=DATASET_TYPE,
        parameters={"file": CONSUMER_RESOURCE_NAME},
        autoconfigure=False,
    )
    yield None
    with suppress(Exception):
        setup_cmempy_user_access(context=TestUserContext())
        os.remove("kafka_performance_project.zip")
        delete_project(PROJECT_NAME)


@pytest.fixture()
def entities_project():
    """Provides the DI build project incl. assets."""
    setup_cmempy_user_access(context=TestUserContext())
    project_name = "kafka_entities_perf_project"
    with suppress(Exception):
        delete_project(project_name)
    make_new_project(project_name)
    yield project_name
    setup_cmempy_user_access(context=TestUserContext())
    delete_project(project_name)


@pytest.fixture()
def json_dataset_project():
    """Provides the DI build project incl. assets."""
    setup_cmempy_user_access(context=TestUserContext())

    project_name = "kafka_json_perf_project"
    with suppress(Exception):
        delete_project(project_name)

    with requests.get(url=JSON_PROJECT_LINK, timeout=10, stream=True) as response:
        with open("kafka_json_perf_project.zip", "wb") as project_file:
            shutil.copyfileobj(response.raw, project_file)

    validation_response = upload_project("kafka_json_perf_project.zip")
    import_id = validation_response["projectImportId"]
    project_id = validation_response["projectId"]

    import_from_upload_start(import_id=import_id, project_id=project_id, overwrite_existing=True)
    # loop until "success" boolean is in status response
    status = import_from_upload_status(import_id)
    while "success" not in status:
        status = import_from_upload_status(import_id)

    make_new_dataset(
        project_name=project_id,
        dataset_name="json_dataset_result",
        dataset_type="json",
        parameters={"file": "json_dataset_result.json"},
        autoconfigure=False,
    )
    yield project_id
    with suppress(Exception):
        setup_cmempy_user_access(context=TestUserContext())
        os.remove("kafka_json_perf_project.zip")
        delete_project(project_id)


@needs_cmem
@needs_kafka
def test_perf_kafka_producer_consumer_xml_dataset(xml_dataset_project, topic) -> None:
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
        message_limit=-1,
    ).execute([], TestExecutionContext(project_id=PROJECT_NAME))

    # Ensure producer and consumer are working properly
    resource, _ = get_resource_from_dataset(
        dataset_id=f"{PROJECT_NAME}:{CONSUMER_DATASET_NAME}",
        context=TestUserContext(),
    )
    with resource as consumer_file:
        consumer_file.raw.decode_content = True
        assert XMLUtils.get_elements_len_from_stream(consumer_file.raw) == 286918


@needs_cmem
@needs_kafka
def test_perf_kafka_producer_consumer_with_entities(entities_project, topic) -> None:
    """Test plugin execution for Plain Kafka"""
    no_of_entities = 1000000
    entities = RandomValues(
        random_function="token_urlsafe", number_of_entities=no_of_entities
    ).execute(context=TestExecutionContext())
    # Producer
    KafkaProducerPlugin(
        message_dataset=None,
        bootstrap_servers=KAFKA_CONFIG["bootstrap_server"],
        security_protocol=KAFKA_CONFIG["security_protocol"],
        sasl_mechanisms=KAFKA_CONFIG["sasl_mechanisms"],
        sasl_username=KAFKA_CONFIG["sasl_username"],
        sasl_password=KAFKA_CONFIG["sasl_password"],
        kafka_topic=topic,
    ).execute([entities], TestExecutionContext(project_id=entities_project))

    # Consumer
    consumer_entities = KafkaConsumerPlugin(
        message_dataset=None,
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
    assert (
        consumer_entities.schema.type_uri
        == "https://github.com/eccenca/cmem-plugin-kafka#PlainMessage"
    )
    assert len(consumer_entities.schema.paths) == 5
    for _ in consumer_entities.entities:
        count += 1

    assert count == no_of_entities


@needs_cmem
@needs_kafka
def test_perf_kafka_producer_consumer_with_json_dataset(json_dataset_project, topic) -> None:
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
    setup_cmempy_user_access(context=TestUserContext())
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
        context=TestUserContext(),
    )
    with resource as json_file:
        count = 0
        data = json_stream.requests.load(json_file)
        for _ in data:
            count += 1
        assert count == 1000000
