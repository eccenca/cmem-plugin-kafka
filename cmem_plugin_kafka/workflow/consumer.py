"""Kafka consumer plugin module"""
from typing import Sequence, Dict, Any, Optional

from cmem.cmempy.workspace.tasks import get_task
from cmem_plugin_base.dataintegration.context import ExecutionContext, ExecutionReport
from cmem_plugin_base.dataintegration.description import PluginParameter, Plugin
from cmem_plugin_base.dataintegration.entity import Entities
from cmem_plugin_base.dataintegration.parameter.choice import ChoiceParameterType
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin
from cmem_plugin_base.dataintegration.types import IntParameterType, BoolParameterType
from cmem_plugin_base.dataintegration.utils import write_to_dataset, \
    setup_cmempy_user_access
from confluent_kafka import KafkaError

from cmem_plugin_kafka.constants import (
    SECURITY_PROTOCOLS,
    SASL_MECHANISMS,
    AUTO_OFFSET_RESET,
    BOOTSTRAP_SERVERS_DESCRIPTION,
    SECURITY_PROTOCOL_DESCRIPTION,
    SASL_ACCOUNT_DESCRIPTION,
    SASL_PASSWORD_DESCRIPTION,
    CLIENT_ID_DESCRIPTION,
    LOCAL_CONSUMER_QUEUE_MAX_SIZE_DESCRIPTION,
    XML_SAMPLE,
    JSON_SAMPLE, MESSAGE_LIMIT_DESCRIPTION, DISABLE_COMMIT_DESCRIPTION,
)
from cmem_plugin_kafka.utils import (
    KafkaConsumer,
    validate_kafka_config,
    get_kafka_statistics,
    get_default_client_id,
    DatasetParameterType,
)
from cmem_plugin_kafka.kafka_handlers import (
    KafkaEntitiesDataHandler,
    KafkaJSONDataHandler,
    KafkaXMLDataHandler,
    KafkaDatasetHandler,
)

CONSUMER_GROUP_DESCRIPTION = """
When a topic is consumed by consumers in the same group, every record will be delivered
to only one consumer of that group.
If all the consumers of a topic are labeled the same consumer group, then the
records will effectively be load-balanced over these consumers.
If all the consumer of a topic are labeled different consumer groups, then each
record will be broadcast to all the consumers.

When the Group Id field is empty, the plugin defaults to DNS:PROJECT ID:TASK ID.
"""

AUTO_OFFSET_RESET_DESCRIPTION = """
What to do when there is no initial offset in Kafka or if the current offset does
not exist any more on the server (e.g. because that data has been deleted).

- `earliest` will fetch the whole topic beginning from the oldest record.
- `latest` will receive nothing but will get any new records on the next run.
"""


@Plugin(
    label="Kafka Consumer (Receive Messages)",
    plugin_id="cmem_plugin_kafka-ReceiveMessages",
    description="Reads messages from a Kafka topic and saves it to a "
    "messages dataset (Consumer).",
    documentation=f"""This workflow operator uses the Kafka Consumer API to receive
messages from a [Apache Kafka](https://kafka.apache.org/).

Need to specify a topic to receive messages from the desired Kafka topic.
All messages received from the topic will be stored as entities and, if messages
were in XML format, can be saved in an XML dataset. Similarly, if the messages were
in JSON format, they can be saved in a JSON dataset.

A sample response from the consumer will appear as follows.

<details>
  <summary>Sample XML Response</summary>
{XML_SAMPLE}
</details>
<details>
  <summary>Sample XML Response</summary>
{JSON_SAMPLE}
</details>

""",
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
            description="The name of the category/feed where messages were"
            " published.",
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

    # pylint: disable=too-many-instance-attributes
    def __init__(
        self,
        message_dataset: str,
        bootstrap_servers: str,
        security_protocol: str,
        sasl_mechanisms: str,
        sasl_username: str,
        sasl_password: str,
        kafka_topic: str,
        auto_offset_reset: str,
        group_id: str = "",
        client_id: str = "",
        local_consumer_queue_size: int = 5000,
        message_limit: int = 100000,
        disable_commit: bool = False
    ) -> None:
        if not isinstance(bootstrap_servers, str):
            raise ValueError("Specified server id is invalid")
        self.message_dataset = message_dataset
        self.bootstrap_servers = bootstrap_servers
        self.security_protocol = security_protocol
        self.sasl_mechanisms = sasl_mechanisms
        self.sasl_username = sasl_username
        self.sasl_password = sasl_password
        self.kafka_topic = kafka_topic
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self.client_id = client_id
        self.local_consumer_queue_size = local_consumer_queue_size
        self.message_limit = int(message_limit)
        self.disable_commit = bool(disable_commit)
        self._kafka_stats: dict = {}

    def metrics_callback(self, json: str):
        """sends producer metrics to server"""
        self._kafka_stats = get_kafka_statistics(json_data=json)
        for key, value in self._kafka_stats.items():
            self.log.info(f"kafka-stats: {key:10} - {value:10}")

    def error_callback(self, err: KafkaError):
        """Error callback"""
        self.log.info(f"kafka-error:{err}")
        if err.code() == -193:  # -193 -> _RESOLVE
            raise err

    def get_config(self, project_id: str = "", task_id: str = "") -> Dict[str, Any]:
        """construct and return kafka connection configuration"""
        default_client_id = get_default_client_id(
            project_id=project_id, task_id=task_id
        )
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": self.security_protocol,
            "enable.auto.commit": False,
            "auto.offset.reset": self.auto_offset_reset,
            "group.id": self.group_id if self.group_id else default_client_id,
            "client.id": self.client_id if self.client_id else default_client_id,
            "statistics.interval.ms": "1000",
            "queued.max.messages.kbytes": self.local_consumer_queue_size,
            # "stats_cb": self.metrics_callback,
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

    def validate(self):
        """Validate parameters"""
        validate_kafka_config(self.get_config(), self.kafka_topic, self.log)

    def execute(
        self, inputs: Sequence[Entities], context: ExecutionContext
    ) -> Optional[Entities]:
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
        task_meta_data = get_task(
            project=context.task.project_id(), task=self.message_dataset
        )
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
            file_resource=handler,
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
