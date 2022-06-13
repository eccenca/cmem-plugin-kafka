"""Kafka utils modules"""
import logging
import re
from xml.sax.handler import ContentHandler  # nosec B406
from xml.sax.saxutils import escape  # nosec B406
from confluent_kafka import Producer


class KafkaProducer:
    """Kafka producer wrapper over conflunet producer"""

    def __init__(self, config: dict, topic: str):
        """Create Producer instance"""
        self._producer = Producer(config)
        self._topic = topic

    def process(self, data: str):
        """Produce message to topic."""
        self._producer.produce(self._topic, data)

    def poll(self, timeout):
        """Polls the producer for events and calls the corresponding callbacks"""
        self._producer.poll(timeout)


class KafkaMessageHandler(ContentHandler):
    """Custom Callback Kafka XML content handler"""

    def __init__(self, kafka_producer: KafkaProducer, plugin_logger=None):
        super().__init__()
        self._message = ""
        self._kafka_producer = kafka_producer
        self._level = 0
        self._no_of_children = 0
        self._no_of_success_messages = 0
        # if plugin logger is None, use default logger.
        if plugin_logger is not None:
            self._log = plugin_logger
        else:
            self._log = logging

    @staticmethod
    def attrs_s(attrs):
        """ This generates the XML attributes from an element attribute list """
        attribute_list = ['']
        for item in attrs.items():
            attribute_list.append(f'{item[0]}="{escape(item[1])}"')

        return ' '.join(attribute_list)

    def startElement(self, name, attrs):
        """Call when an element starts"""
        self._level += 1

        if name == 'Message' and self._level == 2:
            self.rest_for_next_message()
        else:
            open_tag = f'<{name}{self.attrs_s(attrs)}>'
            self._message += open_tag

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
                final_message = re.sub(r'>[ \n]+<', '><', self._message)
                final_message = re.sub(r'[\n ]+$', '', final_message)
                self._no_of_success_messages += 1
                self._kafka_producer.process(final_message)
            else:
                self._log.error("Not able to process this message. "
                                "Reason: Identified more than one children.")

            self.rest_for_next_message()
        else:
            end_tag = f'</{name}>'
            self._message += end_tag
        self._level -= 1

    def characters(self, content: str):
        """Call when a character is read"""
        self._message += content

    def endDocument(self):
        """End of the file"""
        self._kafka_producer.poll(1000)

    def rest_for_next_message(self):
        """To reset _message"""
        self._message = '<?xml version="1.0" encoding="UTF-8"?>'
        self._no_of_children = 0

    def get_success_messages_count(self) -> int:
        """Return count of the successful messages"""
        return self._no_of_success_messages
