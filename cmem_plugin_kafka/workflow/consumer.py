"""Kafka consumer plugin module"""
from collections.abc import Sequence
from typing import Any, BinaryIO

from cmem.cmempy.api import request
from cmem.cmempy.workspace.projects.datasets.dataset import get_dataset_file_uri
from cmem.cmempy.workspace.tasks import get_task
from cmem_plugin_base.dataintegration.context import ExecutionContext, ExecutionReport, UserContext
from cmem_plugin_base.dataintegration.description import Plugin, PluginParameter
from cmem_plugin_base.dataintegration.entity import Entities
from cmem_plugin_base.dataintegration.parameter.choice import ChoiceParameterType
from cmem_plugin_base.dataintegration.parameter.password import Password, PasswordParameterType
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin
from cmem_plugin_base.dataintegration.ports import FixedNumberOfInputs, FixedSchemaPort
from cmem_plugin_base.dataintegration.types import BoolParameterType, IntParameterType
from cmem_plugin_base.dataintegration.utils import setup_cmempy_user_access, split_task_id
from confluent_kafka import KafkaError
from requests import Response

from cmem_plugin_kafka.constants import (
    AUTO_OFFSET_RESET,
    AUTO_OFFSET_RESET_DESCRIPTION,
    BOOTSTRAP_SERVERS_DESCRIPTION,
    CLIENT_ID_DESCRIPTION,
    CONSUMER_GROUP_DESCRIPTION,
    DISABLE_COMMIT_DESCRIPTION,
    LOCAL_CONSUMER_QUEUE_MAX_SIZE_DESCRIPTION,
    MESSAGE_LIMIT_DESCRIPTION,
    PLUGIN_DOCUMENTATION,
    SASL_ACCOUNT_DESCRIPTION,
    SASL_MECHANISMS,
    SASL_PASSWORD_DESCRIPTION,
    SECURITY_PROTOCOL_DESCRIPTION,
    SECURITY_PROTOCOLS,
)
from cmem_plugin_kafka.kafka_handlers import (
    KafkaDatasetHandler,
    KafkaEntitiesDataHandler,
    KafkaJSONDataHandler,
    KafkaXMLDataHandler,
)
from cmem_plugin_kafka.utils import (
    DatasetParameterType,
    KafkaConsumer,
    get_default_client_id,
    get_kafka_statistics,
    validate_kafka_config,
)


@Plugin(
    label="Kafka Consumer (Receive Messages)",
    plugin_id="cmem_plugin_kafka-ReceiveMessages",
    description="Reads messages from a Kafka topic and saves it to a "
    "messages dataset (Consumer).",
    documentation=PLUGIN_DOCUMENTATION,
    parameters=[
        PluginParameter(
            name="bootstrap_servers",
            label="Bootstrap Server",
            description=BOOTSTRAP_SERVERS_DESCRIPTION,
        ),
        PluginParameter(
            name="security_protocol",
            label="Security Protocol",
            description=SECURITY_PROTOCOL_DESCRIPTION,
            param_type=ChoiceParameterType(SECURITY_PROTOCOLS),
            default_value="PLAINTEXT",
        ),
        PluginParameter(
            name="kafka_topic",
            label="Topic",
            description="The name of the category/feed where messages were" " published.",
        ),
        PluginParameter(
            name="message_dataset",
            label="Messages Dataset",
            description="Where do you want to save the messages?"
            " The dropdown lists usable datasets from the current"
            " project only. In case you miss your dataset, check for"
            " the correct type (XML/JSON) and build project.",
            param_type=DatasetParameterType(dataset_type="xml,json"),
            default_value="",
            advanced=True,
        ),
        PluginParameter(
            name="sasl_mechanisms",
            label="SASL Mechanisms",
            param_type=ChoiceParameterType(SASL_MECHANISMS),
            advanced=True,
            default_value="PLAIN",
        ),
        PluginParameter(
            name="sasl_username",
            label="SASL Account",
            advanced=True,
            default_value="",
            description=SASL_ACCOUNT_DESCRIPTION,
        ),
        PluginParameter(
            name="sasl_password",
            label="SASL Password",
            param_type=PasswordParameterType(),
            advanced=True,
            default_value="",
            description=SASL_PASSWORD_DESCRIPTION,
        ),
        PluginParameter(
            name="auto_offset_reset",
            label="Auto Offset Reset",
            param_type=ChoiceParameterType(AUTO_OFFSET_RESET),
            advanced=True,
            default_value="latest",
            description=AUTO_OFFSET_RESET_DESCRIPTION,
        ),
        PluginParameter(
            name="group_id",
            label="Consumer Group Name",
            description=CONSUMER_GROUP_DESCRIPTION,
            advanced=True,
            default_value="",
        ),
        PluginParameter(
            name="client_id",
            label="Client Id",
            advanced=True,
            default_value="",
            description=CLIENT_ID_DESCRIPTION,
        ),
        PluginParameter(
            name="local_consumer_queue_size",
            label="Local Consumer Queue Size",
            advanced=True,
            param_type=IntParameterType(),
            default_value=5000,
            description=LOCAL_CONSUMER_QUEUE_MAX_SIZE_DESCRIPTION,
        ),
        PluginParameter(
            name="message_limit",
            label="Message Limit",
            advanced=True,
            param_type=IntParameterType(),
            default_value=100000,
            description=MESSAGE_LIMIT_DESCRIPTION,
        ),
        PluginParameter(
            name="disable_commit",
            label="Disable Commit",
            advanced=True,
            param_type=BoolParameterType(),
            default_value=False,
            description=DISABLE_COMMIT_DESCRIPTION,
        ),
    ],
)
class KafkaConsumerPlugin(WorkflowPlugin):
    """Kafka Consumer Plugin"""

    def __init__(  # noqa: PLR0913
        self,
        message_dataset: str,
        bootstrap_servers: str,
        security_protocol: str,
        sasl_mechanisms: str,
        sasl_username: str,
        sasl_password: str | Password,
        kafka_topic: str,
        auto_offset_reset: str,
        group_id: str = "",
        client_id: str = "",
        local_consumer_queue_size: int = 5000,
        message_limit: int = 100000,
        disable_commit: bool = False,  # noqa: FBT001, FBT002
    ) -> None:
        if not isinstance(bootstrap_servers, str):
            raise TypeError("Specified server id is invalid")
        self.message_dataset = message_dataset
        self.bootstrap_servers = bootstrap_servers
        self.security_protocol = security_protocol
        self.sasl_mechanisms = sasl_mechanisms
        self.sasl_username = sasl_username
        self.sasl_password = (
            sasl_password if isinstance(sasl_password, str) else sasl_password.decrypt()
        )
        self.kafka_topic = kafka_topic
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self.client_id = client_id
        self.local_consumer_queue_size = local_consumer_queue_size
        self.message_limit = int(message_limit)
        self.disable_commit = bool(disable_commit)
        self._kafka_stats: dict = {}
        self._set_ports()

    def _set_ports(self) -> None:
        """Define input/output ports based on the configuration"""
        self.input_ports = FixedNumberOfInputs([])
        # no output port if dataset is selected
        if self.message_dataset:
            self.output_port = None
        else:
            # output port with fixed schema
            self.output_port = FixedSchemaPort(schema=KafkaEntitiesDataHandler.get_schema())

    def metrics_callback(self, json: str) -> None:
        """Send producer metrics to server"""
        self._kafka_stats = get_kafka_statistics(json_data=json)
        for key, value in self._kafka_stats.items():
            self.log.info(f"kafka-stats: {key:10} - {value:10}")

    def error_callback(self, err: KafkaError) -> None:
        """Error callback"""
        self.log.info(f"kafka-error:{err}")
        if err.code() == -193:  # noqa: PLR2004 # -193 -> _RESOLVE
            raise err

    def get_config(self, project_id: str = "", task_id: str = "") -> dict[str, Any]:
        """Construct and return kafka connection configuration"""
        default_client_id = get_default_client_id(project_id=project_id, task_id=task_id)
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": self.security_protocol,
            "enable.auto.commit": False,
            "auto.offset.reset": self.auto_offset_reset,
            "group.id": self.group_id if self.group_id else default_client_id,
            "client.id": self.client_id if self.client_id else default_client_id,
            "statistics.interval.ms": "1000",
            "queued.max.messages.kbytes": self.local_consumer_queue_size,
            "error_cb": self.error_callback,
        }
        if self.security_protocol.startswith("SASL"):
            config.update(
                {
                    "sasl.mechanisms": self.sasl_mechanisms,
                    "sasl.username": self.sasl_username,
                    "sasl.password": self.sasl_password,
                }
            )
        return config

    def validate(self) -> None:
        """Validate parameters"""
        validate_kafka_config(self.get_config(), self.kafka_topic, self.log)

    def execute(self, inputs: Sequence[Entities], context: ExecutionContext) -> Entities | None:
        """Execute the workflow plugin on a given collection of entities."""
        _ = inputs
        self.log.info("Kafka Consumer Started")
        self.validate()

        kafka_consumer = KafkaConsumer(
            config=self.get_config(
                project_id=context.task.project_id(), task_id=context.task.task_id()
            ),
            commit_offset=not self.disable_commit,
            topic=self.kafka_topic,
            log=self.log,
            context=context,
        )
        kafka_consumer.fetch_limit = self.message_limit
        kafka_consumer.subscribe()
        if not self.message_dataset:
            return KafkaEntitiesDataHandler(
                context=context, plugin_logger=self.log, kafka_consumer=kafka_consumer
            ).consume_messages()
        setup_cmempy_user_access(context=context.user)
        task_meta_data = get_task(project=context.task.project_id(), task=self.message_dataset)
        if task_meta_data["data"]["type"] == "json":
            handler: KafkaDatasetHandler = KafkaJSONDataHandler(
                context=context, plugin_logger=self.log, kafka_consumer=kafka_consumer
            )
        else:
            handler = KafkaXMLDataHandler(
                context=context, plugin_logger=self.log, kafka_consumer=kafka_consumer
            )
        # Prefix project id to dataset name
        self.message_dataset = f"{context.task.project_id()}:{self.message_dataset}"
        write_to_dataset(
            dataset_id=self.message_dataset,
            file_resource=handler,  # type: ignore[arg-type]
            context=context.user,
        )
        context.report.update(
            ExecutionReport(
                entity_count=kafka_consumer.get_success_messages_count(),
                operation="read",
                operation_desc="messages received",
                summary=list(self._kafka_stats.items()),
            )
        )

        return None


def write_to_dataset(
    dataset_id: str, file_resource: BinaryIO, context: UserContext | None = None
) -> Response:
    """Write to a dataset.

    Args:
    ----
        dataset_id (str): The combined task ID.
        file_resource (file stream): Already opened byte file stream
        context (UserContext):
            The user context to setup environment for accessing CMEM with cmempy.

    Returns:
    -------
        requests.Response object

    Raises:
    ------
        ValueError: in case the task ID is not splittable
        ValueError: missing parameter

    """
    setup_cmempy_user_access(context=context)
    project_id, task_id = split_task_id(dataset_id)

    return post_resource(
        project_id=project_id,
        dataset_id=task_id,
        file_resource=file_resource,
    )


def post_resource(project_id: str, dataset_id: str, file_resource: BinaryIO) -> Response:
    """Post a resource to a dataset.

    If the dataset resource already exists, posting a new resource will replace it.

    Args:
    ----
        project_id (str): The ID of the project.
        dataset_id (str): The ID of the dataset.
        file_resource (io Binary Object, optional): The file resource to be uploaded.

    Returns:
    -------
        Response: The response from the request.

    """
    endpoint = get_dataset_file_uri().format(project_id, dataset_id)

    with file_resource as file:
        return request(  # type: ignore[no-any-return]
            endpoint,
            method="PUT",
            stream=True,
            data=file,
        )
