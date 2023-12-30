"""constants module"""
import collections

KAFKA_TIMEOUT = 5
KAFKA_RETRY_COUNT = 3

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
    {"none": "None", "gzip": "gzip", "snappy": "Snappy", "lz4": "LZ4", "zstd": "Zstandard"}
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
    "The credentials for the SASL Account. "  # noqa: S105
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
Setting this to true will disable committing messages after retrival.

This means you will receive the same messages on the next execution (for debugging).
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

CONSUMER_GROUP_DESCRIPTION = """
When a topic is consumed by consumers in the same group, every record will be delivered
to only one consumer of that group.
If all the consumers of a topic are labeled the same consumer group, then the
records will effectively be load-balanced over these consumers.
If all the consumer of a topic are labeled different consumer groups, then each
record will be broadcast to all the consumers.

When the Group Id field is empty, the plugin defaults to DNS:PROJECT ID:TASK ID.
"""

AUTO_OFFSET_RESET_DESCRIPTION = """
What to do when there is no initial offset in Kafka or if the current offset does
not exist any more on the server (e.g. because that data has been deleted).

- `earliest` will fetch the whole topic beginning from the oldest record.
- `latest` will receive nothing but will get any new records on the next run.
"""

PARSE_JSON_LINK = (
    "https://documentation.eccenca.com/latest/"
    "deploy-and-configure/configuration/dataintegration/plugin-reference/#parse-json"
)

PARSE_XML_LINK = (
    "https://documentation.eccenca.com/latest/"
    "deploy-and-configure/configuration/dataintegration/plugin-reference/#parse-xml"
)

PLUGIN_DOCUMENTATION = f"""
This workflow operator uses the Kafka Consumer API
to receive messages from an [Apache Kafka](https://kafka.apache.org/) topic.

Messages received from the topic will be generated as entities with the following
flat schema:

- **key** - the optional key of the message,
- **content** - the message itself as plain text (use other operators, such as
  [Parse JSON]({PARSE_JSON_LINK}) or [Parse XML]({PARSE_XML_LINK}) to process
  complex message content),
- **offset** - the given offset of the message in the topic,
- **ts-production** - the timestamp when the message was written to the topic,
- **ts-consumption** - the timestamp when the message was consumed from the topic.

In order to process the resulting entities, they have to run through a transformation.

As an alternate working mode, messages can be exported directly to a JSON or XML
dataset if you know that the messages on your topic are valid JSON or XML documents
(see Advanced Options > Messages Dataset).

In this case, a sample response from the consumer will appear as follows:

<details>
  <summary>Sample JSON Response</summary>
{JSON_SAMPLE}
</details>
<details>
  <summary>Sample XML Response</summary>
{XML_SAMPLE}
</details>
"""
