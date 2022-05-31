import xml.sax
from xml.sax.saxutils import escape
from confluent_kafka import Producer


class KafkaProducer:
    def __init__(self, config: dict, topic: str):
        # Create Producer instance
        self._producer = Producer(config)
        self._topic = topic

    def process(self, data: str):
        self._producer.produce(self._topic, data)

    def poll(self, timeout):
        self._producer.poll(timeout)


class KafkaMessageHandler(xml.sax.handler.ContentHandler):
    def __init__(self, kafka_producer: KafkaProducer):
        super().__init__()
        self._message = ""
        self._kafka_producer = kafka_producer
        self._level = 0

    @staticmethod
    def attrs_s(attrs):
        """ This generates the XML attributes from an element attribute list """
        attribute_list = ['']
        for item in attrs.items():
            attribute_list.append('%s="%s"' % (item[0], escape(item[1])))

        return ' '.join(attribute_list)

    def startElement(self, tag, attributes):
        """Call when an element starts"""
        if tag == 'Message':
            self._level += 1
            if self._level == 1:
                self.rest_for_next_message()
        elif self._level > 0:
            open_tag = '<%s%s>' % (tag, self.attrs_s(attributes))
            self._message += open_tag

    def endElement(self, tag):
        """Call when an elements end"""

        if tag == "Message" and self._level >= 1:
            self._level -= 1
            if self._level == 0:
                self._kafka_producer.process(self._message)
                self.rest_for_next_message()
        elif self._level > 0:
            end_tag = f'</{tag}>'
            self._message += end_tag

    def characters(self, content: str):
        """Call when a character is read"""
        if self._level > 0:
            self._message += content

    def endDocument(self):
        self._kafka_producer.poll(1000)

    def rest_for_next_message(self):
        """To reset _message"""
        self._message = '<?xml version="1.0" encoding="UTF-8"?>'
