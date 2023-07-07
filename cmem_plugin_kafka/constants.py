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

# Compression Types
COMPRESSION_TYPES = collections.OrderedDict(
    {
        "none": "None",
        "gzip": "gzip",
        "snappy": "Snappy",
        "lz4": "LZ4",
        "zstd": "Zstandard"
    }
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

COMPRESSION_TYPE_DESCRIPTION = """
The compression type for all data generated by the producer.

The default is none (i.e. no compression).
"""

MESSAGE_MAX_SIZE_DESCRIPTION = """
The maximum size of a request message in bytes. This is also effectively a cap on
the maximum record size.

Note that the server has its own cap on record size which may be different from this.
"""

LOCAL_CONSUMER_QUEUE_MAX_SIZE_DESCRIPTION = """
Maximum total message size in kilobytes that the consumer can buffer for
a specific partition. The consumer will stop fetching from the partition
if it hits this limit. This helps prevent consumers from running out of memory.
"""

MESSAGE_LIMIT_DESCRIPTION = """
The maximum number of messages to fetch and process in each run.
If 0 or less, all messages will be fetched.
"""

DISABLE_COMMIT_DESCRIPTION = """
Setting this to false will allow the consumer to commit the offset to kafka.
"""

XML_SAMPLE = """
```xml
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
"""

JSON_SAMPLE = """
```json
[
  {
    "message": {
      "key": "818432-942813-832642-453478",
      "headers": {
        "type": "ADD"
      },
      "content": {
        "location": ["Leipzig"],
        "obstacle": {
          "name": "Iron Bars",
          "order": "1"
        }
      }
    }
  },
  {
    "message": {
      "key": "887428-119918-570674-866526",
      "headers": {
        "type": "REMOVE"
      },
      "content": {
        "comments": "We can pass any json payload here."
      }
    }
  }
]
```
"""
