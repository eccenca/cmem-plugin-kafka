"""Kafka consumer plugin module"""
from typing import Sequence, Dict, Any

from cmem_plugin_base.dataintegration.context import ExecutionContext, ExecutionReport
from cmem_plugin_base.dataintegration.description import PluginParameter, Plugin
from cmem_plugin_base.dataintegration.entity import Entities
from cmem_plugin_base.dataintegration.parameter.choice import ChoiceParameterType
from cmem_plugin_base.dataintegration.parameter.dataset import DatasetParameterType
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin
from cmem_plugin_base.dataintegration.utils import write_to_dataset

from cmem_plugin_kafka.constants import (
    SECURITY_PROTOCOLS,
    SASL_MECHANISMS,
    AUTO_OFFSET_RESET,
    BOOTSTRAP_SERVERS_DESCRIPTION,
    SECURITY_PROTOCOL_DESCRIPTION,
    SASL_ACCOUNT_DESCRIPTION,
    SASL_PASSWORD_DESCRIPTION,
)
from cmem_plugin_kafka.utils import (
    KafkaConsumer,
    validate_kafka_config,
    get_kafka_statistics,
)

CONSUMER_GROUP_DESCRIPTION = """
When a topic is consumed by consumers in the same group, every record will be delivered
to only one consumer of that group.
If all the consumers of a topic are labeled the same consumer group, then the
records will effectively be load-balanced over these consumers.
If all the consumer of a topic are labeled different consumer groups, then each
record will be broadcast to all the consumers.
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
    documentation="""This workflow operator uses the Kafka Consumer API to receive
messages from a [Apache Kafka](https://kafka.apache.org/).

Need to specify a group id to receive messages from a Kafka topic. All messages received
from a topic will be stored into a xml dataset. Sample response from the consumer will
look this.
```
<?xml version="1.0" encoding="utf-8"?>
<KafkaMessages>
  <Message>
    <PurchaseOrder OrderDate="1996-04-06">
      <ShipTo country="string">
        <name>string</name>
      </ShipTo>
    </PurchaseOrder>
  </Message>
  <Message>
    <PurchaseOrder OrderDate="1996-04-06">
      <ShipTo country="string">
        <name>string</name>
      </ShipTo>
    </PurchaseOrder>
  </Message>
</KafkaMessages>
```
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
            name="group_id",
            label="Consumer Group Name",
            description=CONSUMER_GROUP_DESCRIPTION,
        ),
        PluginParameter(
            name="kafka_topic",
            label="Topic",
            description="The name of the category/feed where messages were"
            " published.",
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
            name="message_dataset",
            label="Messages Dataset",
            description="Where do you want to save the messages?"
            " The dropdown lists usable datasets from the current"
            " project only. In case you miss your dataset, check for"
            " the correct type (XML) and build project.",
            param_type=DatasetParameterType(dataset_type="xml"),
            advanced=True,
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
        group_id: str,
        auto_offset_reset: str,
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
        validate_kafka_config(self.get_config(), self.kafka_topic, self.log)
        self._kafka_stats: dict = {}

    def metrics_callback(self, json: str):
        """sends producer metrics to server"""
        self._kafka_stats = get_kafka_statistics(json_data=json)
        for key, value in self._kafka_stats.items():
            self.log.info(f"kafka-stats: {key:10} - {value:10}")

    def get_config(self) -> Dict[str, Any]:
        """construct and return kafka connection configuration"""
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": self.security_protocol,
            "group.id": self.group_id,
            "enable.auto.commit": True,
            "auto.offset.reset": self.auto_offset_reset,
            "client.id": "cmem-plugin-kafka",
            "statistics.interval.ms": "250",
            "stats_cb": self.metrics_callback,
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

    def execute(self, inputs: Sequence[Entities], context: ExecutionContext) -> None:
        self.log.info("Kafka Consumer Started")
        # Prefix project id to dataset name
        self.message_dataset = f"{context.task.project_id()}:{self.message_dataset}"

        kafka_consumer = KafkaConsumer(
            config=self.get_config(),
            topic=self.kafka_topic,
            log=self.log,
            context=context,
        )

        write_to_dataset(
            dataset_id=self.message_dataset,
            file_resource=kafka_consumer,
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
