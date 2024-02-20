"""Module provides a base class for producing messages from data to/from a Kafka topic."""
import hashlib
import json
import re
from abc import ABC
from collections.abc import Generator
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from xml.etree.ElementTree import Element
from xml.sax.saxutils import escape  # nosec B406

import json_stream
import json_stream.requests
from cmem_plugin_base.dataintegration.context import ExecutionContext, ExecutionReport
from cmem_plugin_base.dataintegration.entity import (
    Entities,
    Entity,
    EntityPath,
    EntitySchema,
)
from cmem_plugin_base.dataintegration.plugins import PluginLogger
from defusedxml import ElementTree
from requests import Response

from cmem_plugin_kafka.utils import (
    KafkaConsumer,
    KafkaMessage,
    KafkaProducer,
    get_message_with_json_wrapper,
    get_message_with_xml_wrapper,
)

if TYPE_CHECKING:
    from collections.abc import Sequence


class KafkaDataHandler:
    """Base class for producing messages from data to a Kafka topic.

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
        kafka_producer: KafkaProducer | None = None,
        kafka_consumer: KafkaConsumer | None = None,
    ):
        """Initialize a new KafkaDataHandler instance."""
        self._kafka_producer = kafka_producer
        self._kafka_consumer = kafka_consumer
        self._context: ExecutionContext = context
        self._log: PluginLogger = plugin_logger

    def send_messages(self, data: Response) -> None:
        """Send messages to the Kafka topic from the input data.

        This method splits the input data into individual messages, then sends each
        message as a separate Kafka record.

        :param data: The input data to produce messages from.
        :type data: any
        """
        if not self._kafka_producer:
            return
        messages = self._split_data(data)
        count = 0
        for message in messages:
            self._kafka_producer.process(message)
            count += 1
            if count % 10 == 0:
                self._kafka_producer.poll(0)
                self.update_report()
        self._kafka_producer.flush()

    def _split_data(self, data: Response) -> Generator[KafkaMessage, KafkaMessage, KafkaMessage]:
        """Split the input data into individual messages.

        This method should be implemented by subclasses to handle the specific
        data format.

        :param data: The input data to split into messages.
        :type data: str
        :return: A list of individual messages to send to Kafka.
        :rtype: list
        """
        raise NotImplementedError("Subclass must implement _split_data method")

    def consume_messages(self) -> Generator | None:
        """Consume messages from the Kafka topic and aggregate them into a single object.

        :return: The aggregated object of all consumed messages.
        :rtype: any
        """
        if not self._kafka_consumer:
            return None
        self._kafka_consumer.subscribe()
        return self._aggregate_data()

    def _aggregate_data(self) -> Generator:
        """Aggregate the input data into a single object.

        This method should be implemented by subclasses to handle
        the specific data format.

        :return: The aggregated object of all consumed messages.
        :rtype: any
        """
        raise NotImplementedError("Subclass must implement _aggregate_data method")

    def update_report(self) -> None:
        """Update the plugin report with the current status of the Kafka producer.

        This method creates an ExecutionReport object and updates the plugin report
        with the current status of the Kafka producer, including the number of
        successfully sent messages.
        """
        if not self._kafka_producer:
            return
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
        """Enter the context"""
        return self.consume_messages()

    def __exit__(self, exc_type, exc_val, exc_tb):  # noqa: ANN001
        """Commit and close the Kafka consumer"""
        if not exc_val:
            self._kafka_consumer.commit()
        self._kafka_consumer.close()


class KafkaJSONDataHandler(KafkaDatasetHandler):
    """A class for producing messages from JSON Dataset to a Kafka topic.

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
        kafka_producer: KafkaProducer | None = None,
        kafka_consumer: KafkaConsumer | None = None,
    ):
        """Initialize a new KafkaJSONDataHandler instance."""
        plugin_logger.info("Initialize KafkaJSONDataHandler")
        super().__init__(context, plugin_logger, kafka_producer, kafka_consumer)

    def _split_data(self, data: Response) -> Generator:
        for message in json_stream.requests.load(data):
            _message = json_stream.to_standard_types(message["message"])
            key = _message["key"] if "key" in _message else None
            headers = _message["headers"] if "headers" in _message else {}
            content = _message["content"]
            yield KafkaMessage(
                key=key, value=json.dumps(content) if content else "", headers=headers
            )

    def _aggregate_data(self) -> Generator:
        """Generate json file with kafka messages"""
        if not self._kafka_consumer:
            raise ValueError("Kafka consumer is None")
        try:
            yield b"["
            count = 0
            for message in self._kafka_consumer.poll():
                if count > 0:
                    yield b","
                yield get_message_with_json_wrapper(message).encode()
                count += 1
            yield b"]"
        except json.decoder.JSONDecodeError as ex:
            raise ValueError("Kafka Message is not in expected format ") from ex


class KafkaXMLDataHandler(KafkaDatasetHandler):
    """A class for producing messages from XML Dataset to a Kafka topic.

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
        kafka_producer: KafkaProducer | None = None,
        kafka_consumer: KafkaConsumer | None = None,
    ):
        """Initialize a new KafkaXMLDataHandler instance."""
        self._level: int = 0
        self._no_of_children: int = 0
        self._message: KafkaMessage = KafkaMessage()
        plugin_logger.info("Initialize KafkaJSONDataHandler")
        super().__init__(context, plugin_logger, kafka_producer, kafka_consumer)

    def _aggregate_data(self) -> Generator:
        """Generate xml file with kafka messages"""
        if not self._kafka_consumer:
            raise ValueError("Kafka consumer is None")
        yield b'<?xml version="1.0" encoding="UTF-8"?>\n'
        yield b"<KafkaMessages>"
        for message in self._kafka_consumer.poll():
            yield get_message_with_xml_wrapper(message).encode()

        yield b"</KafkaMessages>"

    def _split_data(self, data: Response) -> Generator:
        data.raw.decode_content = True
        context = ElementTree.iterparse(data.raw, events=("start", "end"))
        # get the root element
        event, root = next(context, None)  # type: ignore[misc]
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
    def attrs_s(attrs: dict) -> str:
        """Generate the XML attributes from an element attribute list"""
        attribute_list = [""]
        attribute_list.extend([f'{item[0]}="{escape(item[1])}"' for item in attrs.items()])
        return " ".join(attribute_list)

    @staticmethod
    def get_key(attrs: dict) -> str | None:
        """Get message key attribute from element attributes list"""
        for item in attrs.items():
            if item[0] == "key":
                return escape(item[1])
        return None

    def start_element(self, element: Element) -> None:
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
        if self._level == 2:  # noqa: PLR2004
            self._no_of_children += 1

        if text:
            self._message.value += text

    def end_element(self, element: Element) -> KafkaMessage | None:
        """Call when an elements end"""
        name = element.tag
        if name == "Message" and self._level == 1:
            if self._no_of_children == 0:
                self._message.value = ""
                self._level -= 1
                return self._message
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
                "Not able to process this message. " "Reason: Identified more than one children."
            )

        else:
            end_tag = f"</{name}>"
            self._message.value += end_tag
        self._level -= 1
        return None

    def rest_for_next_message(self, attrs: dict) -> None:
        """To reset _message"""
        value = '<?xml version="1.0" encoding="UTF-8"?>'
        key = self.get_key(attrs)
        self._message = KafkaMessage(key=key, value=value)
        self._no_of_children = 0


class KafkaEntitiesDataHandler(KafkaDataHandler):
    """A class for producing messages from Entities to a Kafka topic.

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
        kafka_producer: KafkaProducer | None = None,
        kafka_consumer: KafkaConsumer | None = None,
    ):
        """Initialize a new KafkaEntitiesDataHandler instance"""
        plugin_logger.info("Initialize KafkaEntitiesDataHandler")
        super().__init__(context, plugin_logger, kafka_producer, kafka_consumer)
        self._schema: EntitySchema = None

    def _split_data(self, data: Entities) -> Generator:
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

    def _aggregate_data(self) -> Entities:
        self._schema = self.get_schema()
        entities = self.get_entities()
        return Entities(entities=entities, schema=self._schema)

    @staticmethod
    def get_schema() -> EntitySchema:
        """Return kafka message schema paths"""
        schema_paths = [
            EntityPath(path="key"),
            EntityPath(path="content"),
            EntityPath(path="offset"),
            EntityPath(path="ts-production"),
            EntityPath(path="ts-consumption"),
        ]
        return EntitySchema(
            type_uri="https://github.com/eccenca/cmem-plugin-kafka#PlainMessage",
            paths=schema_paths,
        )

    def _get_paths(self, values: dict) -> list:
        self._log.info(f"_get_paths: Values dict {values}")
        return list(values.keys())

    def get_entities(self) -> Generator:
        """Generate the entities from kafka messages"""
        if not self._kafka_consumer:
            raise ValueError("Kafka consumer is None")

        for message in self._kafka_consumer.poll():
            yield self._get_entity(message)

        self._kafka_consumer.commit()
        self._kafka_consumer.close()

    @staticmethod
    def _get_entity(message: KafkaMessage) -> Entity:
        sha256 = hashlib.sha256(message.key.encode() if message.key else b"").hexdigest()
        entity_uri = f"urn:hash::sha256:{sha256}"
        values = [
            [message.key],
            [message.value],
            [f"{message.offset}"],
            [f"{message.timestamp}"],
            [f"{int(round(datetime.now(tz=UTC).timestamp() * 1000))}"],
        ]
        return Entity(uri=entity_uri, values=values)
