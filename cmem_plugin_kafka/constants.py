"""constants module"""
import collections

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

# Auto Offset Resets
AUTO_OFFSET_RESET = collections.OrderedDict(
    {
        "earliest": "automatically reset the offset to the earliest offset (earliest)",
        "latest": "automatically reset the offset to the latest offset (latest)",
        "none": "throw exception to the consumer if no previous offset "
        "is found for the consumer's group (none)",
    }
)

BOOTSTRAP_SERVERS_DESCRIPTION = (
    "This is URL of one of the Kafka brokers. The task fetches the initial metadata "
    "about your Kafka cluster from this URL. "
)

SECURITY_PROTOCOL_DESCRIPTION = (
    "Which security mechanisms need to be applied to connect? Use SASL in case you "
    "connect to a Confluent platform. Use PLAINTEXT in case you connect to a plain "
    "Kafka, which is available inside your VPN. In case you use SASL, you also need to "
    "specify your SASL account and password in the advanced section. "
)
