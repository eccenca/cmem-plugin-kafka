"""Kafka utils modules"""
import re
from xml.sax.handler import ContentHandler  # nosec B406
from xml.sax.saxutils import escape  # nosec B406

from cmem_plugin_base.dataintegration.context import ExecutionContext, ExecutionReport
from cmem_plugin_base.dataintegration.plugins import PluginLogger
from confluent_kafka import Producer


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
