"""siemens playground plugin module"""

import io
from defusedxml import sax

from cmem.cmempy.workspace.projects.resources.resource import get_resource_response
from cmem.cmempy.workspace.tasks import get_task
from cmem_plugin_base.dataintegration.description import Plugin, PluginParameter
from cmem_plugin_base.dataintegration.parameter.dataset import DatasetParameterType
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin
from cmem_plugin_base.dataintegration.utils import (
    split_task_id,
    setup_cmempy_super_user_access
)
from .utils import KafkaProducer, KafkaMessageHandler


@Plugin(
    label="Siemens Kafka",
    description="Plugin to manage producer and consumer messages of a topic",
    documentation="""This example workflow operator produces and
    consumes messages from a topic of cluster on a given
    bootstrap server.
    Parameters to connect bootstrap server.
    - 'bootstrap_servers': server id to be connected
    - 'security_protocol': specify the security protocol while connecting the server
    - 'sasl_mechanisms': specify the sasl mechanisms
    - 'sasl_username': specify the username to connect to the server.
    - 'sasl_password': specify the password to connect to the server.
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
            default_value="SASL_SSL"
        ),
        PluginParameter(
            name="sasl_mechanisms",
            label="SASL Mechanisms",
            description="specify the sasl mechanisms",
            default_value="PLAIN"
        ),
        PluginParameter(
            name="sasl_username",
            label="SASL username",
            description="specify the username to connect to the server.",
        ),
        PluginParameter(
            name="sasl_password",
            label="SASL password",
            description="specify the password to connect to the server.",
        ),
        PluginParameter(
            name="kafka_topic",
            label="kafka Topic",
            description="specify the topic to post messages",
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

    def execute(self, inputs=()):
        self.log.info("Start Kafka Plugin")
        kafka_connection_config = {'bootstrap.servers': self.bootstrap_servers,
                                   'security.protocol': self.security_protocol,
                                   "sasl.mechanisms": self.sasl_mechanisms,
                                   'sasl.username': self.sasl_username,
                                   'sasl.password': self.sasl_password}

        parser = sax.make_parser()

        # override the default ContextHandler
        handler = KafkaMessageHandler(KafkaProducer(kafka_connection_config,
                                                    self.kafka_topic),
                                      plugin_logger=self.log)
        parser.setContentHandler(handler)

        with self.get_resource_from_dataset() as response:
            data = io.StringIO(response.text)

        with data as xml_stream:
            parser.parse(xml_stream)

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
