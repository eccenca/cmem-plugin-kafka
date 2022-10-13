"""Kafka consumer plugin module"""
import io
from typing import Sequence, Dict, Any

from cmem_plugin_base.dataintegration.context import ExecutionContext, ExecutionReport
from cmem_plugin_base.dataintegration.description import PluginParameter, Plugin
from cmem_plugin_base.dataintegration.entity import Entities
from cmem_plugin_base.dataintegration.parameter.choice import ChoiceParameterType
from cmem_plugin_base.dataintegration.parameter.dataset import DatasetParameterType
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin
from cmem_plugin_base.dataintegration.utils import write_to_dataset

from ..constants import (
    SECURITY_PROTOCOLS,
    SASL_MECHANISMS,
    AUTO_OFFSET_RESET,
    BOOTSTRAP_SERVERS_DESCRIPTION,
    SECURITY_PROTOCOL_DESCRIPTION,
)
from ..utils import (
    KafkaConsumer,
    validate_kafka_config,
    get_kafka_statistics,
    get_message_with_wrapper
)


@Plugin(
    label="Receive messages from Apache Kafka",
    plugin_id="cmem_plugin_kafka-ReceiveMessages",
    description="Receives multiple messages from a Apache Kafka server.",
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
        ),
        PluginParameter(
            name="group_id",
            label="Group",
            description="The id is to specify the name of the consumer group of a "
            "[Kafka consumer](https://docs.confluent.io/kafka-clients/python/current/overview.html#ak-consumer) belongs to",  # noqa: E501 # pylint: disable=line-too-long
        ),
        PluginParameter(
            name="kafka_topic",
            label="Topic",
            description="The topic is a category/feed name to which the messages are"
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
            name="sasl_username", label="SASL Account", advanced=True, default_value=""
        ),
        PluginParameter(
            name="sasl_password", label="SASL Password", advanced=True, default_value=""
        ),
        PluginParameter(
            name="auto_offset_reset",
            label="Auto Offset Reset",
            param_type=ChoiceParameterType(AUTO_OFFSET_RESET),
            advanced=True,
            default_value="latest",
        ),
        PluginParameter(
            name="message_dataset",
            label="Messages Dataset",
            description="Where do you want to save the messages?"
            " The dropdown lists usable datasets from the current"
            " project only. In case you miss your dataset, check for"
            " the correct type (XML) and build project).",
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
        self.log.info(json)
        self._kafka_stats = get_kafka_statistics(json_data=json)
        for key, value in self._kafka_stats.items():
            self.log.info(f'kafka-stats: {key:10} - {value:10}')

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

        _kafka_consumer = KafkaConsumer(
            config=self.get_config(), topic=self.kafka_topic, _log=self.log
        )
        _kafka_consumer.subscribe()
        count = 0
        file_resource = io.StringIO()
        file_resource.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        file_resource.write("<KafkaMessages>")
        for message in _kafka_consumer.poll():
            count += 1
            file_resource.write(get_message_with_wrapper(message))

        file_resource.write("</KafkaMessages>")

        _kafka_consumer.close()
        context.report.update(
            ExecutionReport(
                entity_count=count,
                operation="read",
                operation_desc="messages received from kafka server",
                summary=list(self._kafka_stats.items())
            )
        )
        file_resource.seek(0)
        write_to_dataset(
            dataset_id=self.message_dataset,
            file_resource=file_resource,
            context=context.user,
        )
