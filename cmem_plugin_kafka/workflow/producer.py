"""Kafka producer plugin module"""
from collections.abc import Sequence
from typing import Any

from cmem_plugin_base.dataintegration.context import (
    ExecutionContext,
    ExecutionReport,
)
from cmem_plugin_base.dataintegration.description import Plugin, PluginParameter
from cmem_plugin_base.dataintegration.entity import Entities
from cmem_plugin_base.dataintegration.parameter.choice import ChoiceParameterType
from cmem_plugin_base.dataintegration.parameter.password import Password, PasswordParameterType
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin
from cmem_plugin_base.dataintegration.ports import FixedNumberOfInputs
from cmem_plugin_base.dataintegration.types import IntParameterType
from confluent_kafka import KafkaError

from cmem_plugin_kafka.constants import (
    BOOTSTRAP_SERVERS_DESCRIPTION,
    CLIENT_ID_DESCRIPTION,
    COMPRESSION_TYPE_DESCRIPTION,
    COMPRESSION_TYPES,
    JSON_SAMPLE,
    MESSAGE_MAX_SIZE_DESCRIPTION,
    SASL_ACCOUNT_DESCRIPTION,
    SASL_MECHANISMS,
    SASL_PASSWORD_DESCRIPTION,
    SECURITY_PROTOCOL_DESCRIPTION,
    SECURITY_PROTOCOLS,
    XML_SAMPLE,
)
from cmem_plugin_kafka.kafka_handlers import (
    KafkaDataHandler,
    KafkaEntitiesDataHandler,
    KafkaJSONDataHandler,
    KafkaXMLDataHandler,
)
from cmem_plugin_kafka.utils import (
    DatasetParameterType,
    KafkaProducer,
    get_default_client_id,
    get_kafka_statistics,
    get_resource_from_dataset,
    validate_kafka_config,
)

TOPIC_DESCRIPTION = """
The name of the category/feed to which the messages will be published.

Note that you may create this topic in advance before publishing messages to it.
This is especially true for a kafka cluster hosted at
[confluent.cloud](https://confluent.cloud).
"""


@Plugin(
    label="Kafka Producer (Send Messages)",
    plugin_id="cmem_plugin_kafka-SendMessages",
    description="Reads a messages dataset and sends records to a" " Kafka topic (Producer).",
    documentation=f"""This workflow operator uses the Kafka Producer API to send
messages to a [Apache Kafka](https://kafka.apache.org/).

Accepts entities as input, and, if desired, accepts a pre-constructed XML/JSON dataset,
which is transformed into messages and sent to a designated Kafka topic based
on configuration.

<details>
  <summary>Sample XML format</summary>

  An example XML document is shown below. This document will be sent as two messages
  to the configured topic. Each message is created as a proper XML document.

{XML_SAMPLE}
</details>

<details>
  <summary>Sample JSON format</summary>

  An example JSON document is shown below. This document will be sent as two messages
  to the configured topic. Each message is created as a proper JSON document.

{JSON_SAMPLE}
</details>

""",
    parameters=[
        PluginParameter(
            name="message_dataset",
            label="Messages Dataset",
            description="Where do you want to retrieve the messages from?"
            " The dropdown lists usable datasets from the current"
            " project only. In case you miss your dataset, check for"
            " the correct type (XML/JSON) and build project)."
            " The messages will be retrieved from the entities if no dataset is provided.",
            param_type=DatasetParameterType(dataset_type="xml,json"),
            default_value="",
            advanced=True,
        ),
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
        PluginParameter(name="kafka_topic", label="Topic", description=TOPIC_DESCRIPTION),
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
            name="client_id",
            label="Client Id",
            advanced=True,
            default_value="",
            description=CLIENT_ID_DESCRIPTION,
        ),
        PluginParameter(
            name="message_max_bytes",
            label="Maximum Message Size",
            advanced=True,
            param_type=IntParameterType(),
            default_value="1048576",
            description=MESSAGE_MAX_SIZE_DESCRIPTION,
        ),
        PluginParameter(
            name="compression_type",
            label="Compression Type",
            advanced=True,
            param_type=ChoiceParameterType(COMPRESSION_TYPES),
            default_value="none",
            description=COMPRESSION_TYPE_DESCRIPTION,
        ),
    ],
)
class KafkaProducerPlugin(WorkflowPlugin):
    """Kafka Producer Plugin"""

    def __init__(  # noqa: PLR0913
        self,
        message_dataset: str,
        bootstrap_servers: str,
        security_protocol: str,
        sasl_mechanisms: str,
        sasl_username: str,
        sasl_password: str | Password,
        kafka_topic: str,
        client_id: str = "",
        message_max_bytes: str = "1048576",
        compression_type: str = "none",
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
        self.client_id = client_id
        self.message_max_bytes = message_max_bytes
        self.compression_type = compression_type
        self._kafka_stats: dict = {}
        self._set_ports()

    def _set_ports(self) -> None:
        """Define input/output ports based on the configuration"""
        self.output_port = None
        # no input port if dataset is selected
        if self.message_dataset:
            self.input_ports = FixedNumberOfInputs([])

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
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": self.security_protocol,
            "client.id": self.client_id
            if self.client_id
            else get_default_client_id(project_id=project_id, task_id=task_id),
            "statistics.interval.ms": "1000",
            "message.max.bytes": int(self.message_max_bytes),
            "compression.type": self.compression_type,
            "stats_cb": self.metrics_callback,
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

    def execute(self, inputs: Sequence[Entities], context: ExecutionContext) -> None:
        """Execute the workflow plugin on a given collection of entities."""
        self.log.info("Start Kafka Plugin")
        self.validate()

        # override the default ContextHandler
        producer = KafkaProducer(
            config=self.get_config(
                project_id=context.task.project_id(), task_id=context.task.task_id()
            ),
            topic=self.kafka_topic,
        )

        context.report.update(
            ExecutionReport(entity_count=0, operation="wait", operation_desc="messages sent")
        )

        if self.message_dataset:
            # Prefix project id to dataset name
            self.message_dataset = f"{context.task.project_id()}:{self.message_dataset}"

            resource, _ = get_resource_from_dataset(
                dataset_id=self.message_dataset, context=context.user
            )
            if _["data"]["type"] == "json":
                handler: KafkaDataHandler = KafkaJSONDataHandler(
                    context=context, plugin_logger=self.log, kafka_producer=producer
                )
            else:
                handler = KafkaXMLDataHandler(
                    context=context, plugin_logger=self.log, kafka_producer=producer
                )
            with resource as response:
                handler.send_messages(response)
        else:
            entities_handler = KafkaEntitiesDataHandler(
                context=context,
                plugin_logger=self.log,
                kafka_producer=producer,
            )
            for entities in inputs:
                entities_handler.send_messages(entities)

        context.report.update(
            ExecutionReport(
                entity_count=producer.get_success_messages_count(),
                operation="write",
                operation_desc="messages sent",
                summary=list(self._kafka_stats.items()),
            )
        )
