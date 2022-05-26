"""siemens playground plugin module"""

from cmem_plugin_base.dataintegration.description import Plugin, PluginParameter
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin
from cmem_plugin_siekafka.config import SASL_MECHANISMS, SECURITY_PROTOCOL


@Plugin(
    label="Siemens Kafka Playground",
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
            name="bootstrap_servers",
            label="Bootstrap Servers",
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
        )
    ]
)
class KafkaPlugin(WorkflowPlugin):
    """Kafka Plugin"""

    def __init__(
            self,
            bootstrap_servers: str,
            sasl_username: str,
            sasl_password: str,
            security_protocol: str = SECURITY_PROTOCOL,
            sasl_mechanisms: str = SASL_MECHANISMS,
    ) -> None:
        if not isinstance(bootstrap_servers, str):
            raise ValueError('Specified server id is invalid')
        self.bootstrap_servers = bootstrap_servers
        self.security_protocol = security_protocol
        self.sasl_mechanisms = sasl_mechanisms
        self.sasl_username = sasl_username
        self.sasl_password = sasl_password

    def execute(self, inputs=()):
        self.log.info("Start creating random values.")
        self.log.info(f"Config length: {len(self.config.get())}")
        self.config.get()

        raise ValueError('Data is fully valid')
