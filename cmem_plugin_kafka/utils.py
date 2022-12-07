"""Kafka utils modules"""
import json
import re
from typing import Dict, Any, Iterator, Optional, Sequence
from urllib.parse import urlparse
from xml.sax.handler import ContentHandler  # nosec B406
from xml.sax.saxutils import escape  # nosec B406

from cmem.cmempy.config import get_cmem_base_uri
from cmem.cmempy.workspace.projects.resources.resource import get_resource_response
from cmem.cmempy.workspace.tasks import get_task
from cmem_plugin_base.dataintegration.context import (
    ExecutionContext,
    ExecutionReport,
    UserContext,
)
from cmem_plugin_base.dataintegration.entity import (
    Entities,
    Entity,
    EntityPath,
    EntitySchema,
)
from cmem_plugin_base.dataintegration.plugins import PluginLogger
from cmem_plugin_base.dataintegration.utils import (
    setup_cmempy_user_access,
    split_task_id,
)
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
from confluent_kafka.admin import AdminClient, TopicMetadata, ClusterMetadata
from defusedxml import ElementTree

from cmem_plugin_kafka.constants import KAFKA_TIMEOUT


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

    def __init__(self, key: Optional[str] = None, value: str = ""):
        self.value: str = value
        self.key: Optional[str] = key


class KafkaProducer:
    """Kafka producer wrapper over confluent producer"""

    def __init__(self, config: dict, topic: str):
        """Create Producer instance"""
        self._producer = Producer(config)
        self._topic = topic
        self._no_of_success_messages: int = 0

    def process(self, message: KafkaMessage):
        """Produce message to topic."""
        self._no_of_success_messages += 1
        self._producer.produce(self._topic, value=message.value, key=message.key)

    def poll(self, timeout):
        """Polls the producer for events and calls the corresponding callbacks"""
        self._producer.poll(timeout)

    def flush(self, timeout=KAFKA_TIMEOUT):
        """Wait for all messages in the Producer queue to be delivered."""
        prev = 0
        while True:
            messages_in_queue = self._producer.flush(timeout=timeout)
            if prev == messages_in_queue:
                break
            prev = messages_in_queue

    def get_success_messages_count(self) -> int:
        """Return count of the successful messages"""
        return self._no_of_success_messages


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
        self._first_message: Optional[KafkaMessage] = None
        self._schema: EntitySchema = None

    def __enter__(self):
        return self.get_xml_payload()

    def get_schema(self):
        """Return kafka message schema paths"""
        message = self.get_first_message()
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
        if self._first_message:
            yield self._get_entity(self._first_message)

        for message in self.poll():
            yield self._get_entity(message)

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

    def get_xml_payload(self) -> Iterator[bytes]:
        """generate xml file with kafka messages"""
        yield '<?xml version="1.0" encoding="UTF-8"?>\n'.encode()
        yield "<KafkaMessages>".encode()
        for message in self.poll():
            yield get_message_with_wrapper(message).encode()

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

    def get_first_message(self):
        """Get the first message from kafka subscribed topic"""
        if self._first_message:
            return self._first_message
        count = 0
        while True:
            msg = self._consumer.poll(timeout=KAFKA_TIMEOUT)
            count += 1
            if msg or count > 3:
                break

        if msg is None:
            self._log.info("get_first_message: Messages are empty")
        else:
            self._first_message = KafkaMessage(
                key=msg.key().decode("utf-8") if msg.key() else "",
                value=msg.value().decode("utf-8"),
            )
        return self._first_message

    def poll(self) -> Iterator[KafkaMessage]:
        """Polls the consumer for events and calls the corresponding callbacks"""
        while True:
            msg = self._consumer.poll(timeout=KAFKA_TIMEOUT)
            if msg is None:
                self._log.info("Messages are empty")
                break
            if msg.error():
                self._log.error(f"Consumer poll Error:{msg.error()}")
                raise KafkaException(msg.error())

            self._no_of_success_messages += 1
            kafka_message = KafkaMessage(
                key=msg.key().decode("utf-8") if msg.key() else "",
                value=msg.value().decode("utf-8"),
            )
            if not self._first_message:
                self._first_message = kafka_message
            if not self._no_of_success_messages % 10:
                self._context.report.update(
                    ExecutionReport(
                        entity_count=self._no_of_success_messages,
                        operation="read",
                        operation_desc="messages received",
                    )
                )
            yield kafka_message

    def close(self):
        """Closes the consumer once all messages were received."""
        self._consumer.close()


class KafkaXMLHandler(ContentHandler):
    """Custom Callback Kafka XML content handler"""

    def __init__(
        self, kafka_producer: KafkaProducer, context: ExecutionContext, plugin_logger
    ):
        super().__init__()
        self._level: int = 0
        self._no_of_children: int = 0
        self._kafka_producer = kafka_producer
        self._context: ExecutionContext = context
        self._log: PluginLogger = plugin_logger
        self._message: KafkaMessage = KafkaMessage()

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
                # Remove newline and white space between open and close tag
                final_message = re.sub(r">[ \n]+<", "><", self._message.value)
                # Remove new and white space at the end of the xml
                self._message.value = re.sub(r"[\n ]+$", "", final_message)

                self._kafka_producer.process(self._message)
                if self._kafka_producer.get_success_messages_count() % 10 == 0:
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

    def update_report(self):
        """Update the plugin report with current status"""
        self._context.report.update(
            ExecutionReport(
                entity_count=self._kafka_producer.get_success_messages_count(),
                operation="wait",
                operation_desc="messages sent",
            )
        )


class KafkaEntitiesHandler:
    """Custom Callback Kafka XML content handler"""

    def __init__(
        self, kafka_producer: KafkaProducer, context: ExecutionContext, plugin_logger
    ):
        self._kafka_producer = kafka_producer
        self._context: ExecutionContext = context
        self._log: PluginLogger = plugin_logger

    def process(self, entities: Entities):
        """Process entities"""
        for message_dict in self.get_dict(entities):
            kafka_payload = json.dumps(message_dict, indent=4)
            self._kafka_producer.process(KafkaMessage(key=None, value=kafka_payload))
            if self._kafka_producer.get_success_messages_count() % 10 == 0:
                self._kafka_producer.poll(0)
                self.update_report()
        self._kafka_producer.flush()

    def update_report(self):
        """Update the plugin report with current status"""
        self._context.report.update(
            ExecutionReport(
                entity_count=self._kafka_producer.get_success_messages_count(),
                operation="wait",
                operation_desc="messages sent",
            )
        )

    def get_dict(self, entities: Entities) -> Iterator[Dict[str, str]]:
        """get dict from entities"""
        self._log.info("Generate dict from entities")

        paths = entities.schema.paths
        type_uri = entities.schema.type_uri
        result: dict[str, Any] = {"schema": {"type_uri": type_uri}}
        for entity in entities.entities:
            values: dict[str, Sequence[str]] = {}
            for i, path in enumerate(paths):
                values[path.path] = list(entity.values[i])
            result["entity"] = {"uri": entity.uri, "values": values}
            yield result


def get_default_client_id(project_id: str, task_id: str):
    """return dns:projectId:taskId when client id is empty"""
    base_url = get_cmem_base_uri()
    dns = urlparse(base_url).netloc
    return f"{dns}:{project_id}:{task_id}"


def validate_kafka_config(config: Dict[str, Any], topic: str, log: PluginLogger):
    """Validate kafka configuration"""
    admin_client = AdminClient(config)
    cluster_metadata: ClusterMetadata = admin_client.list_topics(
        topic=topic, timeout=KAFKA_TIMEOUT
    )

    topic_meta: TopicMetadata = cluster_metadata.topics[topic]
    kafka_error: KafkaError = topic_meta.error

    if kafka_error and kafka_error.code() == KafkaError.LEADER_NOT_AVAILABLE:
        raise ValueError(
            "The topic you configured, was just created. Save again if this ok for you."
            " Otherwise, change the topic name."
        )
    if kafka_error:
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
    is_xml(message.value)
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
        key: ",".join(stats[key].keys())
        if isinstance(stats[key], dict)
        else f"{stats[key]}"
        for key in interested_keys
    }


def is_xml(value: str):
    """Check value is xml string or not"""
    try:
        ElementTree.fromstring(value)
    except ElementTree.ParseError as exc:
        raise ValueError("Kafka message is not in Valid XML format") from exc
