"""Kafka plugin module"""

import io
import uuid
from typing import Optional

from cmem.cmempy.workspace.projects.resources.resource import get_resource_response
from cmem.cmempy.workspace.tasks import get_task
from cmem_plugin_base.dataintegration.description import PluginParameter, Plugin
from cmem_plugin_base.dataintegration.entity import (
    Entities, Entity, EntityPath, EntitySchema
)
from cmem_plugin_base.dataintegration.parameter.dataset import DatasetParameterType
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin
from cmem_plugin_base.dataintegration.types import Autocompletion, StringParameterType
from cmem_plugin_base.dataintegration.utils import (
    split_task_id,
    setup_cmempy_super_user_access
)
from confluent_kafka.admin import AdminClient, ClusterMetadata, TopicMetadata
from defusedxml import sax

from .utils import KafkaProducer, KafkaMessageHandler

KAFKA_TIMEOUT = 5

# Security Protocols
SECURITY_PROTOCOLS = [
    ["SASL_SSL", "SASL authenticated, SSL channel"],
    ["SASL_PLAINTEXT", "SASL authenticated, non-encrypted channel"],
    ["PLAINTEXT", "Un-authenticated, non-encrypted channel"]
]
# SASL Mechanisms
SASL_MECHANISMS = [
    ["", "--None--"],
    ["PLAIN", "Authentication based on username and passwords"]
]


class DropDown(StringParameterType):
    """Drop Down for a list"""

    allow_only_autocompleted_values: bool = True

    autocomplete_value_with_labels: bool = True

    drop_list: list[list[str]] = [[]]

    def __init__(
            self, drop_list=None
    ):
        """Dataset parameter type."""
        if drop_list is None:
            drop_list = [[]]
        self.drop_list = drop_list

    def autocomplete(
            self, query_terms: list[str], project_id: Optional[str] = None
    ) -> list[Autocompletion]:
        return [
            Autocompletion(value=name, label=f"{name}: {description}")
            if name != '' else Autocompletion(value='', label='--None--')
            for name, description in self.drop_list
        ]


def generate_entities(messages_count: int) -> Entities:
    """generates entities"""
    # create the entities
    entities = []
    for _ in range(messages_count):
        entity_uri = f"urn:uuid:{str(uuid.uuid4())}"
        entities.append(Entity(uri=entity_uri, values=[[]]))

    # create the schema
    paths = []
    for path_no in range(1):
        path_uri = f"https://example.org/vocab/KafkaValuePath/{path_no}"
        paths.append(EntityPath(path=path_uri))
    schema = EntitySchema(
        type_uri="https://example.org/vocab/KafkaValueRow",
        paths=paths,
    )

    return Entities(entities=entities, schema=schema)


@Plugin(
    label="Kafka",
    description="Plugin to manage producer and consumer messages of a topic",
    documentation="""This example workflow operator produces and
consumes messages from a topic of cluster on a given
bootstrap server.

Parameters to connect bootstrap server.

- `bootstrap_servers`: server id to be connected
- `security_protocol`: specify the security protocol while connecting the server
- `sasl_mechanisms`: specify the sasl mechanisms
- `sasl_username`: specify the username to connect to the server.
- `sasl_password`: specify the password to connect to the server.
- `kafka_topic`: specify the topic for the data to be created.
""",
    parameters=[
        PluginParameter(
            name="message_dataset",
            label="Dataset",
            description="Dateset name to retrieve Kafka XML Messages",
            param_type=DatasetParameterType(dataset_type="xml")
        ),
        PluginParameter(
            name="bootstrap_servers",
            label="Bootstrap Server",
            description="server id to be connected",
        ),
        PluginParameter(
            name="security_protocol",
            label="Security Protocol",
            description="specify the security protocol while connecting the server",
            param_type=DropDown(drop_list=SECURITY_PROTOCOLS)
        ),
        PluginParameter(
            name="kafka_topic",
            label="kafka Topic",
            description="specify the topic to post messages",
        ),
        PluginParameter(
            name="sasl_mechanisms",
            label="SASL Mechanisms",
            description="specify the sasl mechanisms",
            param_type=DropDown(drop_list=SASL_MECHANISMS),
            advanced=True,
            default_value=''
        ),
        PluginParameter(
            name="sasl_username",
            label="SASL username",
            description="specify the username to connect to the server.",
            advanced=True,
            default_value=''

        ),
        PluginParameter(
            name="sasl_password",
            label="SASL password",
            description="specify the password to connect to the server.",
            advanced=True,
            default_value=''
        )
    ]
)
class KafkaPlugin(WorkflowPlugin):
    """Kafka Plugin"""

    def __init__(
            self,
            message_dataset: str,
            bootstrap_servers: str,
            security_protocol: str,
            sasl_mechanisms: str,
            sasl_username: str,
            sasl_password: str,
            kafka_topic: str
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
        self.validate_connection()

    def validate_connection(self):
        """Validate kafka configuration"""
        admin_client = AdminClient(self.get_config())
        cluster_metadata: ClusterMetadata = \
            admin_client.list_topics(topic=self.kafka_topic,
                                     timeout=KAFKA_TIMEOUT)

        topic_meta: TopicMetadata = cluster_metadata.topics[self.kafka_topic]
        kafka_error = topic_meta.error

        if kafka_error is not None:
            raise kafka_error

    def get_config(self):
        """construct and return kafka connection configuration"""
        config = {'bootstrap.servers': self.bootstrap_servers,
                  'security.protocol': self.security_protocol}
        if self.security_protocol.startswith('SASL'):
            config.update({
                "sasl.mechanisms": self.sasl_mechanisms,
                'sasl.username': self.sasl_username,
                'sasl.password': self.sasl_password
            })
        return config

    def execute(self, inputs=()) -> Entities:
        self.log.info("Start Kafka Plugin")
        self.validate_connection()
        parser = sax.make_parser()

        # override the default ContextHandler
        handler = KafkaMessageHandler(KafkaProducer(self.get_config(),
                                                    self.kafka_topic),
                                      plugin_logger=self.log)
        parser.setContentHandler(handler)

        with self.get_resource_from_dataset() as response:
            data = io.StringIO(response.text)

        with data as xml_stream:
            parser.parse(xml_stream)

        count = handler.get_success_messages_count()
        return generate_entities(messages_count=count)

    def get_resource_from_dataset(self):
        """Get resource from dataset"""
        setup_cmempy_super_user_access()
        project_id, task_id = split_task_id(self.message_dataset)
        task_meta_data = get_task(
            project=project_id,
            task=task_id
        )
        resource_name = str(task_meta_data['data']["parameters"]["file"]["value"])

        return get_resource_response(project_id, resource_name)
