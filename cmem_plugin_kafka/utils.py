"""Kafka utils modules"""
import json
import re
from typing import Dict, Any, Iterator
from xml.sax.handler import ContentHandler  # nosec B406
from xml.sax.saxutils import escape  # nosec B406

from cmem.cmempy.workspace.projects.resources.resource import get_resource_response
from cmem.cmempy.workspace.tasks import get_task
from cmem_plugin_base.dataintegration.context import (
    ExecutionContext,
    ExecutionReport,
    UserContext,
)
from cmem_plugin_base.dataintegration.plugins import PluginLogger
from cmem_plugin_base.dataintegration.utils import (
    setup_cmempy_user_access,
    split_task_id,
)
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, TopicMetadata, ClusterMetadata

from .constants import KAFKA_TIMEOUT


# pylint: disable-msg=too-few-public-methods
class KafkaMessage:
    """
    A class used to represent/hold a Kafka Message key and value

    ...

    Attributes
    ----------
    key : str
        Kafka message key
    value : str
        Kafka message payload
    """

    def __init__(self, key: str = "", value: str = ""):
        self.value: str = value
        self.key: str = key


class KafkaProducer:
    """Kafka producer wrapper over confluent producer"""

    def __init__(self, config: dict, topic: str):
        """Create Producer instance"""
        self._producer = Producer(config)
        self._topic = topic

    def process(self, message: KafkaMessage):
        """Produce message to topic."""
        self._producer.produce(self._topic, value=message.value, key=message.key)

    def poll(self, timeout):
        """Polls the producer for events and calls the corresponding callbacks"""
        self._producer.poll(timeout)

    def flush(self):
        """Wait for all messages in the Producer queue to be delivered."""
        self._producer.flush()


class KafkaConsumer:
    """Kafka consumer wrapper over confluent consumer"""

    def __init__(
        self, config: dict, topic: str, log: PluginLogger, context: ExecutionContext
    ):
        """Create consumer instance"""
        self._consumer = Consumer(config)
        self._context = context

        self._topic = topic
        self._log = log
        self._no_of_success_messages = 0

    def __enter__(self):
        self.subscribe()
        return self.get_xml_payload()

    def get_xml_payload(self) -> Iterator[bytes]:
        """generate xml file with kafka messages"""
        yield '<?xml version="1.0" encoding="UTF-8"?>\n'.encode()
        yield "<KafkaMessages>".encode()
        for message in self.poll():
            self._no_of_success_messages += 1
            yield get_message_with_wrapper(message).encode()
            if not self._no_of_success_messages % 10:
                self._context.report.update(
                    ExecutionReport(
                        entity_count=self._no_of_success_messages,
                        operation="read",
                        operation_desc="messages received",
                    )
                )

        yield "</KafkaMessages>".encode()

    def __exit__(self, exc_type, exc_value, exc_tb):
        # Exception handling here
        self._consumer.close()

    def get_success_messages_count(self) -> int:
        """Return count of the successful messages"""
        return self._no_of_success_messages

    def subscribe(self):
        """Subscribes to a topic to consume messages"""
        self._consumer.subscribe(topics=[self._topic])

    def poll(self) -> Iterator[KafkaMessage]:
        """Polls the consumer for events and calls the corresponding callbacks"""
        try:
            while True:
                msg = self._consumer.poll(timeout=KAFKA_TIMEOUT)
                if msg is None:
                    self._log.info("Messages are empty")
                    break
                if msg.error():
                    if msg.error().code() == KafkaError.BROKER_NOT_AVAILABLE:
                        self._log.error("kafka broker is not available")
                    else:
                        self._log.error(f"KAFKA ERROR: {msg.error()}")
                yield KafkaMessage(
                    key=msg.key().decode("utf-8") if msg.key() else "",
                    value=msg.value().decode("utf-8"),
                )
        except KafkaError as kafka_error:
            self._log.info(f"Kafka Error{kafka_error.code()}")

    def close(self):
        """Closes the consumer once all messages were received."""
        self._consumer.close()


class KafkaMessageHandler(ContentHandler):
    """Custom Callback Kafka XML content handler"""

    _message: KafkaMessage
    _log: PluginLogger
    _context: ExecutionContext
    _level: int = 0
    _no_of_children: int = 0
    _no_of_success_messages: int = 0

    def __init__(
        self, kafka_producer: KafkaProducer, context: ExecutionContext, plugin_logger
    ):
        super().__init__()

        self._kafka_producer = kafka_producer
        self._context = context
        self._log = plugin_logger
        self._message = KafkaMessage()

    @staticmethod
    def attrs_s(attrs):
        """This generates the XML attributes from an element attribute list"""
        attribute_list = [""]
        for item in attrs.items():
            attribute_list.append(f'{item[0]}="{escape(item[1])}"')

        return " ".join(attribute_list)

    @staticmethod
    def get_key(attrs):
        """get message key attribute from element attributes list"""
        for item in attrs.items():
            if item[0] == "key":
                return escape(item[1])
        return None

    def startElement(self, name, attrs):
        """Call when an element starts"""
        self._level += 1

        if name == "Message" and self._level == 2:
            self.rest_for_next_message(attrs)
        else:
            open_tag = f"<{name}{self.attrs_s(attrs)}>"
            self._message.value += open_tag

        # Number of child for Message tag
        if self._level == 3:
            self._no_of_children += 1

    def endElement(self, name):
        """Call when an elements end"""

        if name == "Message" and self._level == 2:
            # If number of children are more than 1,
            # We can not construct proper kafka xml message.
            # So, log the error message
            if self._no_of_children == 1:
                self._no_of_success_messages += 1
                # Remove newline and white space between open and close tag
                final_message = re.sub(r">[ \n]+<", "><", self._message.value)
                # Remove new and white space at the end of the xml
                self._message.value = re.sub(r"[\n ]+$", "", final_message)

                self._kafka_producer.process(self._message)
                if self._no_of_success_messages % 10 == 0:
                    self._kafka_producer.poll(0)
                    self.update_report()
            else:
                self._log.error(
                    "Not able to process this message. "
                    "Reason: Identified more than one children."
                )

        else:
            end_tag = f"</{name}>"
            self._message.value += end_tag
        self._level -= 1

    def characters(self, content: str):
        """Call when a character is read"""
        self._message.value += content

    def endDocument(self):
        """End of the file"""
        self._kafka_producer.flush()

    def rest_for_next_message(self, attrs):
        """To reset _message"""
        value = '<?xml version="1.0" encoding="UTF-8"?>'
        key = self.get_key(attrs)
        self._message = KafkaMessage(key, value)
        self._no_of_children = 0

    def get_success_messages_count(self) -> int:
        """Return count of the successful messages"""
        return self._no_of_success_messages

    def update_report(self):
        """Update the plugin report with current status"""
        self._context.report.update(
            ExecutionReport(
                entity_count=self.get_success_messages_count(),
                operation="wait",
                operation_desc="messages sent",
            )
        )


def validate_kafka_config(config: Dict[str, Any], topic: str, log: PluginLogger):
    """Validate kafka configuration"""
    admin_client = AdminClient(config)
    cluster_metadata: ClusterMetadata = admin_client.list_topics(
        topic=topic, timeout=KAFKA_TIMEOUT
    )

    topic_meta: TopicMetadata = cluster_metadata.topics[topic]
    kafka_error = topic_meta.error

    if kafka_error is not None:
        raise kafka_error
    log.info("Connection details are valid")


def get_resource_from_dataset(dataset_id: str, context: UserContext):
    """Get resource from dataset"""
    setup_cmempy_user_access(context=context)
    project_id, task_id = split_task_id(dataset_id)
    task_meta_data = get_task(project=project_id, task=task_id)
    resource_name = str(task_meta_data["data"]["parameters"]["file"]["value"])

    return get_resource_response(project_id, resource_name)


def get_message_with_wrapper(message: KafkaMessage) -> str:
    """Wrap kafka message around Message tags"""
    # strip xml metadata
    regex_pattern = "<\\?xml.*\\?>"
    msg_with_wrapper = f'<Message key="{message.key}">'
    # TODO Efficient way to remove xml doc string
    msg_with_wrapper += re.sub(regex_pattern, "", message.value)
    msg_with_wrapper += "</Message>\n"
    return msg_with_wrapper


def get_kafka_statistics(json_data: str) -> dict:
    """Return kafka statistics from json"""
    interested_keys = [
        "name",
        "client_id",
        "type",
        "time",
        "msg_cnt",
        "msg_size",
        "topics",
    ]
    stats = json.loads(json_data)
    return {
        key: ",".join(stats[key]) if isinstance(stats[key], list) else f"{stats[key]}"
        for key in interested_keys
    }
