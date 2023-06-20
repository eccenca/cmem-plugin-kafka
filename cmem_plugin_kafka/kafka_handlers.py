"""
The `kafka_handlers` module provides a base class `KafkaDataHandler` for producing
messages from data to/from a Kafka topic.
"""
import json
import re
from abc import ABC
from typing import Any, Sequence, Optional
from xml.sax.saxutils import escape  # nosec B406

import defusedxml.ElementTree as ET
import json_stream
import json_stream.requests
from cmem_plugin_base.dataintegration.context import ExecutionContext, ExecutionReport
from cmem_plugin_base.dataintegration.entity import (
    Entities,
    EntityPath,
    EntitySchema,
    Entity,
)
from cmem_plugin_base.dataintegration.plugins import PluginLogger

from cmem_plugin_kafka.utils import (
    KafkaProducer,
    KafkaMessage,
    KafkaConsumer,
    get_message_with_json_wrapper,
    get_message_with_xml_wrapper,
)


class KafkaDataHandler:
    """
    Base class for producing messages from data to a Kafka topic.

    :param context: Execution Context to use.
    :type context: ExecutionContext
    :param plugin_logger: Plugin logger instance to use.
    :type plugin_logger: PluginLogger
    :param kafka_producer: Optional Kafka producer instance to use.
    :type kafka_producer: KafkaProducer
    :param kafka_consumer: Optional Kafka consumer instance to use.
    :type kafka_consumer: KafkaConsumer
    """

    def __init__(
        self,
        context: ExecutionContext,
        plugin_logger: PluginLogger,
        kafka_producer: Optional[KafkaProducer] = None,
        kafka_consumer: Optional[KafkaConsumer] = None,
    ):
        """
        Initialize a new KafkaDataHandler instance with the specified
        execution context, logger, and producer instances.
        """
        self._kafka_producer = kafka_producer
        self._kafka_consumer = kafka_consumer
        self._context: ExecutionContext = context
        self._log: PluginLogger = plugin_logger

    def send_messages(self, data):
        """
        Send messages to the Kafka topic from the input data.
        This method splits the input data into individual messages, then sends each
        message as a separate Kafka record.

        :param data: The input data to produce messages from.
        :type data: any
        """
        messages = self._split_data(data)
        count = 0
        for message in messages:
            self._kafka_producer.process(message)
            count += 1
            if count % 10 == 0:
                self._kafka_producer.poll(0)
                self.update_report()
        self._kafka_producer.flush()

    def _split_data(self, data):
        """
        Split the input data into individual messages.
        This method should be implemented by subclasses to handle the specific
        data format.

        :param data: The input data to split into messages.
        :type data: str
        :return: A list of individual messages to send to Kafka.
        :rtype: list
        """
        raise NotImplementedError("Subclass must implement _split_data method")

    def consume_messages(self):
        """
        Consume messages from the Kafka topic and aggregate them into a single object.

        :return: The aggregated object of all consumed messages.
        :rtype: any
        """
        self._kafka_consumer.subscribe()
        return self._aggregate_data()

    def _aggregate_data(self):
        """
        Aggregate the input data into a single object.
        This method should be implemented by subclasses to handle
        the specific data format.

        :return: The aggregated object of all consumed messages.
        :rtype: any
        """
        raise NotImplementedError("Subclass must implement _aggregate_data method")

    def update_report(self):
        """
        Update the plugin report with the current status of the Kafka producer.

        This method creates an ExecutionReport object and updates the plugin report
        with the current status of the Kafka producer, including the number of
        successfully sent messages.
        """
        self._context.report.update(
            ExecutionReport(
                entity_count=self._kafka_producer.get_success_messages_count(),
                operation="wait",
                operation_desc="messages sent",
            )
        )


class KafkaDatasetHandler(KafkaDataHandler, ABC):
    """A Base class for producing messages from Dataset to a Kafka topic."""

    def __enter__(self):
        return self.consume_messages()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_val:
            self._kafka_consumer.commit()
        self._kafka_consumer.close()


class KafkaJSONDataHandler(KafkaDatasetHandler):
    """
    A class for producing messages from JSON Dataset to a Kafka topic.

    :param context: Execution Context to use.
    :type context: ExecutionContext
    :param plugin_logger: Plugin logger instance to use.
    :type plugin_logger: PluginLogger
    :param kafka_producer: Optional Kafka producer instance to use.
    :type kafka_producer: KafkaProducer
    :param kafka_consumer: Optional Kafka consumer instance to use.
    :type kafka_consumer: KafkaConsumer
    """

    def __init__(
        self,
        context: ExecutionContext,
        plugin_logger: PluginLogger,
        kafka_producer: Optional[KafkaProducer] = None,
        kafka_consumer: Optional[KafkaConsumer] = None,
    ):
        """
        Initialize a new KafkaJSONDataHandler instance with the specified
        execution context, logger, and producer instances.
        """
        plugin_logger.info("Initialize KafkaJSONDataHandler")
        super().__init__(context, plugin_logger, kafka_producer, kafka_consumer)

    def _split_data(self, data):
        for message in json_stream.requests.load(data):
            message = json_stream.to_standard_types(message["message"])
            key = message["key"] if "key" in message else None
            headers = message["headers"] if "headers" in message else {}
            content = message["content"]
            yield KafkaMessage(key=key, value=json.dumps(content), headers=headers)

    def _aggregate_data(self):
        """generate json file with kafka messages"""
        try:
            yield "[".encode()
            count = 0
            for message in self._kafka_consumer.poll():
                if count > 0:
                    yield ",".encode()
                yield get_message_with_json_wrapper(message).encode()
                count += 1
            yield "]".encode()
        except json.decoder.JSONDecodeError as ex:
            raise ValueError("Kafka Message is not in expected format ") from ex


class KafkaXMLDataHandler(KafkaDatasetHandler):
    """
    A class for producing messages from XML Dataset to a Kafka topic.

    :param context: Execution Context to use.
    :type context: ExecutionContext
    :param plugin_logger: Plugin logger instance to use.
    :type plugin_logger: PluginLogger
    :param kafka_producer: Optional Kafka producer instance to use.
    :type kafka_producer: KafkaProducer
    :param kafka_consumer: Optional Kafka consumer instance to use.
    :type kafka_consumer: KafkaConsumer
    """

    def __init__(
        self,
        context: ExecutionContext,
        plugin_logger: PluginLogger,
        kafka_producer: Optional[KafkaProducer] = None,
        kafka_consumer: Optional[KafkaConsumer] = None,
    ):
        """
        Initialize a new KafkaXMLDataHandler instance with the specified
        execution context, logger, and producer instances.
        """
        self._level: int = 0
        self._no_of_children: int = 0
        self._message: KafkaMessage = KafkaMessage()
        plugin_logger.info("Initialize KafkaJSONDataHandler")
        super().__init__(context, plugin_logger, kafka_producer, kafka_consumer)

    def _aggregate_data(self):
        """generate xml file with kafka messages"""
        yield '<?xml version="1.0" encoding="UTF-8"?>\n'.encode()
        yield "<KafkaMessages>".encode()
        for message in self._kafka_consumer.poll():
            yield get_message_with_xml_wrapper(message).encode()

        yield "</KafkaMessages>".encode()

    def _split_data(self, data):
        data.raw.decode_content = True
        context = ET.iterparse(data.raw, events=("start", "end"))
        # get the root element
        event, root = next(context, None)
        if not event:
            return
        for event, element in context:
            if event == "start":
                self.start_element(element)
            elif event == "end":
                message = self.end_element(element)
                root.clear()
                if message:
                    yield message

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

    def start_element(self, element):
        """Call when an element starts"""
        name = element.tag
        attrs = element.attrib
        text = element.text
        self._level += 1

        if name == "Message" and self._level == 1:
            self.rest_for_next_message(attrs)
        else:
            open_tag = f"<{name}{self.attrs_s(attrs)}>"
            self._message.value += open_tag

        # Number of child for Message tag
        if self._level == 2:
            self._no_of_children += 1

        if text:
            self._message.value += text

    def end_element(self, element):
        """Call when an elements end"""
        name = element.tag
        if name == "Message" and self._level == 1:
            # If number of children are more than 1,
            # We can not construct proper kafka xml message.
            # So, log the error message
            if self._no_of_children == 1:
                # Remove newline and white space between open and close tag
                final_message = re.sub(r">[ \n]+<", "><", self._message.value)
                # Remove new and white space at the end of the xml
                self._message.value = re.sub(r"[\n ]+$", "", final_message)
                self._level -= 1
                return self._message

            self._log.error(
                "Not able to process this message. "
                "Reason: Identified more than one children."
            )

        else:
            end_tag = f"</{name}>"
            self._message.value += end_tag
        self._level -= 1
        return None

    def rest_for_next_message(self, attrs):
        """To reset _message"""
        value = '<?xml version="1.0" encoding="UTF-8"?>'
        key = self.get_key(attrs)
        self._message = KafkaMessage(key=key, value=value)
        self._no_of_children = 0


class KafkaEntitiesDataHandler(KafkaDataHandler):
    """
    A class for producing messages from Entities to a Kafka topic.

    :param context: Execution Context to use.
    :type context: ExecutionContext
    :param plugin_logger: Plugin logger instance to use.
    :type plugin_logger: PluginLogger
    :param kafka_producer: Optional Kafka producer instance to use.
    :type kafka_producer: KafkaProducer
    :param kafka_consumer: Optional Kafka consumer instance to use.
    :type kafka_consumer: KafkaConsumer
    """

    def __init__(
        self,
        context: ExecutionContext,
        plugin_logger: PluginLogger,
        kafka_producer: Optional[KafkaProducer] = None,
        kafka_consumer: Optional[KafkaConsumer] = None,
    ):
        """
        Initialize a new KafkaEntitiesDataHandler instance with the specified
        execution context, logger, and producer instances.
        """
        plugin_logger.info("Initialize KafkaEntitiesDataHandler")
        super().__init__(context, plugin_logger, kafka_producer, kafka_consumer)
        self._schema: EntitySchema = None

    def _split_data(self, data: Entities):
        self._log.info("Generate dict from entities")
        paths = data.schema.paths
        type_uri = data.schema.type_uri
        result: dict[str, Any] = {"schema": {"type_uri": type_uri}}
        for entity in data.entities:
            values: dict[str, Sequence[str]] = {}
            for i, path in enumerate(paths):
                values[path.path] = list(entity.values[i])
            result["entity"] = {"uri": entity.uri, "values": values}
            kafka_payload = json.dumps(result, indent=4)
            yield KafkaMessage(key=None, value=kafka_payload)

    def _aggregate_data(self):
        try:
            schema = self.get_schema()
            if not schema:
                return None
            entities = self.get_entities()
            return Entities(entities=entities, schema=schema)
        except (KeyError, json.decoder.JSONDecodeError) as ex:
            raise ValueError("Kafka Message is not in expected format") from ex

    def get_schema(self):
        """Return kafka message schema paths"""
        message = self._kafka_consumer.get_first_message()
        if not message:
            return None
        json_payload = json.loads(message.value)
        schema_paths = []
        self._log.info(f'values : {json_payload["entity"]["values"]}')
        for path in self._get_paths(json_payload["entity"]["values"]):
            path_uri = f"{path}"
            schema_paths.append(EntityPath(path=path_uri))
        self._schema = EntitySchema(
            type_uri=json_payload["schema"]["type_uri"],
            paths=schema_paths,
        )
        return self._schema

    def _get_paths(self, values: dict):
        self._log.info(f"_get_paths: Values dict {values}")
        return list(values.keys())

    def get_entities(self):
        """Generate the entities from kafka messages"""
        if self._kafka_consumer.get_first_message():
            yield self._get_entity(self._kafka_consumer.get_first_message())

        for message in self._kafka_consumer.poll():
            yield self._get_entity(message)

        self._kafka_consumer.commit()

    def _get_entity(self, message: KafkaMessage):
        try:
            json_payload = json.loads(message.value)
        except json.decoder.JSONDecodeError as exc:
            raise ValueError("Kafka message in not in valid JSON format") from exc

        entity_uri = json_payload["entity"]["uri"]
        values = [
            json_payload["entity"]["values"].get(_.path) for _ in self._schema.paths
        ]
        return Entity(uri=entity_uri, values=values)
