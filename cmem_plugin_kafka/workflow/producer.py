"""Kafka producer plugin module"""
import collections
from typing import Sequence

from cmem.cmempy.workspace.projects.resources.resource import get_resource_response
from cmem.cmempy.workspace.tasks import get_task
from cmem_plugin_base.dataintegration.context import (
    ExecutionContext,
    ExecutionReport,
    UserContext,
)
from cmem_plugin_base.dataintegration.description import PluginParameter, Plugin
from cmem_plugin_base.dataintegration.entity import Entities
from cmem_plugin_base.dataintegration.parameter.choice import ChoiceParameterType
from cmem_plugin_base.dataintegration.parameter.dataset import DatasetParameterType
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin
from cmem_plugin_base.dataintegration.utils import (
    split_task_id,
    setup_cmempy_user_access,
)
from confluent_kafka.admin import AdminClient, ClusterMetadata, TopicMetadata
from defusedxml import sax

from ..utils import KafkaProducer, KafkaMessageHandler

KAFKA_TIMEOUT = 5

# Security Protocols
SECURITY_PROTOCOLS = collections.OrderedDict(
    {
        "SASL_SSL": "SASL authenticated, SSL channel (SASL_SSL)",
        "PLAINTEXT": "Un-authenticated, non-encrypted channel (PLAINTEXT)",
    }
)
# SASL Mechanisms
SASL_MECHANISMS = collections.OrderedDict(
    {"PLAIN": "Authentication based on username and passwords (PLAIN)"}
)


@Plugin(
    label="Send messages to Apache Kafka",
    plugin_id="cmem_plugin_kafka-SendMessages",
    description="Reads a prepared messages dataset and sends multiple messages to a"
    " Kafka server.",
    documentation="""This workflow operator uses the Kafka Producer API to send
messages to a [Apache Kafka](https://kafka.apache.org/).

The need-to-send data has to be prepared upfront in an XML dataset. The dataset is
parsed into messages and send to a Kafka topic according to the configuration.

An example XML document is shown below. This document will be sent as two messages
to the configured topic. Each message is created as a proper XML document.
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
            name="message_dataset",
            label="Messages Dataset",
            description="Where do you want to retrieve the messages from?"
            " The dropdown lists usable datasets from the current"
            " project only. In case you miss your dataset, check for"
            " the correct type (XML) and build project).",
            param_type=DatasetParameterType(dataset_type="xml"),
        ),
        PluginParameter(
            name="bootstrap_servers",
            label="Bootstrap Server",
            description="This is URL of one of the Kafka brokers. The task"
            " fetches the initial metadata about your Kafka cluster from"
            " this URL.",
        ),
        PluginParameter(
            name="security_protocol",
            label="Security Protocol",
            description="Which security mechanisms need to be applied to connect?"
            " Use SASL in case you connect to a Confluent platform."
            " Use PLAINTEXT in case you connect to a plain Kafka, which"
            " is available inside your VPN."
            " In case you use SASL, you also need to specify your SASL"
            " account and password in the advanced section.",
            param_type=ChoiceParameterType(SECURITY_PROTOCOLS),
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
    ],
)
class KafkaProducerPlugin(WorkflowPlugin):
    """Kafka Producer Plugin"""

    def __init__(
        self,
        message_dataset: str,
        bootstrap_servers: str,
        security_protocol: str,
        sasl_mechanisms: str,
        sasl_username: str,
        sasl_password: str,
        kafka_topic: str,
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
        self.validate_connection()

    def validate_connection(self):
        """Validate kafka configuration"""
        self.log.info("Start validate connection")
        admin_client = AdminClient(self.get_config())
        cluster_metadata: ClusterMetadata = admin_client.list_topics(
            topic=self.kafka_topic, timeout=KAFKA_TIMEOUT
        )

        topic_meta: TopicMetadata = cluster_metadata.topics[self.kafka_topic]
        kafka_error = topic_meta.error

        if kafka_error is not None:
            raise kafka_error
        self.log.info("Connection details are valid")

    def get_config(self):
        """construct and return kafka connection configuration"""
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": self.security_protocol,
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
        self.log.info("Start Kafka Plugin")
        self.validate_connection()
        # Prefix project id to dataset name
        self.message_dataset = f"{context.task.project_id()}:{self.message_dataset}"

        parser = sax.make_parser()

        # override the default ContextHandler
        handler = KafkaMessageHandler(
            KafkaProducer(self.get_config(), self.kafka_topic),
            context,
            plugin_logger=self.log,
        )
        parser.setContentHandler(handler)
        context.report.update(
            ExecutionReport(
                entity_count=0, operation="wait", operation_desc="messages sent"
            )
        )
        with self.get_resource_from_dataset(context=context.user) as response:
            response.raw.decode_content = True
            parser.parse(response.raw)

        context.report.update(
            ExecutionReport(
                entity_count=handler.get_success_messages_count(),
                operation="write",
                operation_desc="messages sent",
            )
        )

    def get_resource_from_dataset(self, context: UserContext):
        """Get resource from dataset"""
        setup_cmempy_user_access(context=context)
        project_id, task_id = split_task_id(self.message_dataset)
        task_meta_data = get_task(project=project_id, task=task_id)
        resource_name = str(task_meta_data["data"]["parameters"]["file"]["value"])

        return get_resource_response(project_id, resource_name)
