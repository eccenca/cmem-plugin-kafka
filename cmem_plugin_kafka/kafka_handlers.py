"""
The `kafka_handlers` module provides a base class `KafkaDataHandler` for producing
messages from data to/from a Kafka topic.
"""
import json
from typing import Any, Sequence

import ijson
from cmem_plugin_base.dataintegration.context import ExecutionContext, ExecutionReport
from cmem_plugin_base.dataintegration.entity import Entities
from cmem_plugin_base.dataintegration.plugins import PluginLogger

from cmem_plugin_kafka.utils import KafkaProducer, KafkaMessage


class KafkaDataHandler:
    """
    Base class for producing messages from data to a Kafka topic.

    :param context: Execution Context to use.
    :type context: ExecutionContext
    :param plugin_logger: Plugin logger instance to use.
    :type plugin_logger: PluginLogger
    :param kafka_producer: Optional Kafka producer instance to use.
    :type kafka_producer: KafkaProducer
    """

    def __init__(
            self, context: ExecutionContext,
            plugin_logger: PluginLogger, kafka_producer: KafkaProducer,
    ):
        """
        Initialize a new KafkaDataHandler instance with the specified
        execution context, logger, and producer instances.
        """
        self._kafka_producer = kafka_producer
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
        for message in messages:
            self._kafka_producer.process(message)
            if self._kafka_producer.get_success_messages_count() % 10 == 0:
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


class KafkaJSONDataHandler(KafkaDataHandler):
    """
    A class for producing messages from JSON Dataset to a Kafka topic.

    :param context: Execution Context to use.
    :type context: ExecutionContext
    :param plugin_logger: Plugin logger instance to use.
    :type plugin_logger: PluginLogger
    :param kafka_producer: Optional Kafka producer instance to use.
    :type kafka_producer: KafkaProducer
    """
    def __init__(
            self, context: ExecutionContext,
            plugin_logger: PluginLogger, kafka_producer: KafkaProducer,
    ):
        """
        Initialize a new KafkaJSONDataHandler instance with the specified
        execution context, logger, and producer instances.
        """
        plugin_logger.info("Initialize KafkaJSONDataHandler")
        super().__init__(context, plugin_logger, kafka_producer)

    def _split_data(self, data):
        messages = ijson.items(data, "item.message")
        for message in messages:
            key = message["key"] if "key" in message else None
            headers = message["headers"] if "headers" in message else {}
            content = message["content"]
            yield KafkaMessage(key=key, value=json.dumps(content), headers=headers)


class KafkaEntitiesDataHandler(KafkaDataHandler):
    """
    A class for producing messages from Entities to a Kafka topic.

    :param context: Execution Context to use.
    :type context: ExecutionContext
    :param plugin_logger: Plugin logger instance to use.
    :type plugin_logger: PluginLogger
    :param kafka_producer: Optional Kafka producer instance to use.
    :type kafka_producer: KafkaProducer
    """
    def __init__(
            self, context: ExecutionContext,
            plugin_logger: PluginLogger, kafka_producer: KafkaProducer,
    ):
        """
        Initialize a new KafkaEntitiesDataHandler instance with the specified
        execution context, logger, and producer instances.
        """
        plugin_logger.info("Initialize KafkaEntitiesDataHandler")
        super().__init__(context, plugin_logger, kafka_producer)

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