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
        "error": "throw exception to the consumer if no previous offset "
        "is found for the consumer's group (error)",
    }
)

BOOTSTRAP_SERVERS_DESCRIPTION = """
This is URL of one of the Kafka brokers.

The task fetches the initial metadata about your Kafka cluster from this URL.
"""

SECURITY_PROTOCOL_DESCRIPTION = """
Which security mechanisms need to be applied to connect?

Use PLAINTEXT in case you connect to a plain Kafka, which is available inside your VPN.

Use SASL in case you connect to a [confluent.cloud](https://confluent.cloud) cluster
(then you also need to specify your SASL credentials in the advanced options section).
"""

SASL_ACCOUNT_DESCRIPTION = (
    "The account identifier for the SASL authentication. "
    "\n\n"
    "In case you are using a [confluent.cloud](https://confluent.cloud) cluster, "
    "this is the API key."
)


SASL_PASSWORD_DESCRIPTION = (
    "The credentials for the SASL Account. "  # nosec
    "\n\n"
    "In case you are using a [confluent.cloud](https://confluent.cloud) cluster, "
    "this is the API secret."
)

CLIENT_ID_DESCRIPTION = """
An optional identifier of a Kafka client (producer/consumer) that is passed
to a Kafka broker with every request.

The sole purpose of this is to be able to track the source of requests beyond just
ip and port by allowing a logical application name to be included in Kafka logs
and monitoring aggregates.

When the Client Id field is empty, the plugin defaults to DNS:PROJECT ID:TASK ID.
"""

LOCAL_CONSUMER_QUEUE_MAX_SIZE_DESCRIPTION = """
Maximum number of kilobytes per topic+partition in the local consumer queue.
"""
