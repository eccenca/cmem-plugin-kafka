"""Kafka consumer plugin module"""
import collections
from typing import Sequence

from cmem.cmempy.workspace.projects.resources.resource import get_resource_response
from cmem.cmempy.workspace.tasks import get_task
from cmem_plugin_base.dataintegration.context import (
    ExecutionContext, UserContext
)
from cmem_plugin_base.dataintegration.description import PluginParameter, Plugin
from cmem_plugin_base.dataintegration.entity import (
    Entities
)
from cmem_plugin_base.dataintegration.parameter.choice import ChoiceParameterType
from cmem_plugin_base.dataintegration.parameter.dataset import DatasetParameterType
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin
from cmem_plugin_base.dataintegration.utils import (
    split_task_id, setup_cmempy_user_access,
)
from confluent_kafka.admin import AdminClient, ClusterMetadata, TopicMetadata

from ..utils import KafkaConsumer

KAFKA_TIMEOUT = 5

# Security Protocols
SECURITY_PROTOCOLS = collections.OrderedDict({
    "SASL_SSL": "SASL authenticated, SSL channel (SASL_SSL)",
    "PLAINTEXT": "Un-authenticated, non-encrypted channel (PLAINTEXT)"
})
# SASL Mechanisms
SASL_MECHANISMS = collections.OrderedDict({
    "PLAIN": "Authentication based on username and passwords (PLAIN)"
})
# Auto Offset Resets
AUTO_OFFSET_RESET = collections.OrderedDict({
    "earliest": "automatically reset the offset to the earliest offset (earliest)",
    "latest": "automatically reset the offset to the latest offset (latest)",
    "none": "throw exception to the consumer if no previous offset "
            "is found for the consumer's group (none)"
})


@Plugin(
    label="Receive messages from Apache Kafka",
    plugin_id="cmem_plugin_kafka-ReceiveMessages",
    description="Receives multiple messages from a Apache Kafka server.",
    documentation="""This workflow operator uses the Kafka Consumer API to receives
messages from a [Apache Kafka](https://kafka.apache.org/).""",
    parameters=[
        PluginParameter(
            name="bootstrap_servers",
            label="Bootstrap Server",
            description="This is URL of one of the Kafka brokers. The task"
                        " fetches the initial metadata about your Kafka cluster from"
                        " this URL."
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
            param_type=ChoiceParameterType(SECURITY_PROTOCOLS)
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
            default_value='PLAIN'
        ),
        PluginParameter(
            name="sasl_username",
            label="SASL Account",
            advanced=True,
            default_value=''

        ),
        PluginParameter(
            name="sasl_password",
            label="SASL Password",
            advanced=True,
            default_value=''
        ),
        PluginParameter(
            name="auto_offset_reset",
            label="Auto Offset Reset",
            param_type=ChoiceParameterType(AUTO_OFFSET_RESET),
            advanced=True,
            default_value='latest'
        ),
        PluginParameter(
            name="message_dataset",
            label="Messages Dataset",
            description="Where do you want to save the messages?"
                        " The dropdown lists usable datasets from the current"
                        " project only. In case you miss your dataset, check for"
                        " the correct type (XML) and build project).",
            param_type=DatasetParameterType(dataset_type='xml'),
            advanced=True
        ),
    ]
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
            auto_offset_reset: str
    ) -> None:
        if not isinstance(bootstrap_servers, str):
            raise ValueError('Specified server id is invalid')
        self.message_dataset = message_dataset
        self.bootstrap_servers = bootstrap_servers
        self.security_protocol = security_protocol
        self.sasl_mechanisms = sasl_mechanisms
        self.sasl_username = sasl_username
        self.sasl_password = sasl_password
        self.kafka_topic = kafka_topic
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self.validate_connection()

    def validate_connection(self):
        """Validate kafka configuration"""
        self.log.info("Start validate consumer connection")
        admin_client = AdminClient(self.get_config())
        cluster_metadata: ClusterMetadata = \
            admin_client.list_topics(topic=self.kafka_topic,
                                     timeout=KAFKA_TIMEOUT)

        topic_meta: TopicMetadata = cluster_metadata.topics[self.kafka_topic]
        kafka_error = topic_meta.error

        if kafka_error is not None:
            raise kafka_error
        self.log.info("Consumer Connection details are valid")

    def get_config(self):
        """construct and return kafka connection configuration"""
        config = {'bootstrap.servers': self.bootstrap_servers,
                  'security.protocol': self.security_protocol,
                  'group.id': self.group_id,
                  'enable.auto.commit': True,
                  'auto.offset.reset': self.auto_offset_reset
                  }
        if self.security_protocol.startswith('SASL'):
            config.update({
                "sasl.mechanisms": self.sasl_mechanisms,
                'sasl.username': self.sasl_username,
                'sasl.password': self.sasl_password,
            })
        return config

    def execute(self, inputs: Sequence[Entities],
                context: ExecutionContext) -> None:
        self.log.info("Kafka Consumer Started")
        self.validate_connection()
        # Prefix project id to dataset name
        self.message_dataset = f'{context.task.project_id()}:{self.message_dataset}'

        _kafka_consumer = KafkaConsumer(config=self.get_config(),
                                        topic=self.kafka_topic,
                                        _log=self.log)
        _kafka_consumer.process()
        _kafka_consumer.poll(dataset_id=self.message_dataset, context=context)

    def get_resource_from_dataset(self, context: UserContext):
        """Get resource from dataset"""
        setup_cmempy_user_access(context=context)
        project_id, task_id = split_task_id(self.message_dataset)
        task_meta_data = get_task(
            project=project_id,
            task=task_id
        )
        resource_name = str(task_meta_data['data']["parameters"]["file"]["value"])

        return get_resource_response(project_id, resource_name)
