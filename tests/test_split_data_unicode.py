"""Regression tests for non-ASCII character handling in _split_data().

These exercise KafkaJSONDataHandler and KafkaEntitiesDataHandler directly,
without a Kafka broker or a Corporate Memory connection, since _split_data()
only transforms in-memory data into KafkaMessage objects.
"""

import json

import httpx
from cmem_plugin_base.dataintegration.entity import Entities, Entity, EntityPath, EntitySchema
from cmem_plugin_base.dataintegration.plugins import PluginLogger

from cmem_plugin_kafka.kafka_handlers import KafkaEntitiesDataHandler, KafkaJSONDataHandler


def test_json_data_handler_split_data_keeps_unicode_characters() -> None:
    """Test that non-ASCII characters from the source dataset are not escaped"""
    payload = [{"message": {"key": "1", "content": {"city": "Köln", "name": "Müller"}}}]
    data = httpx.Response(200, content=json.dumps(payload).encode("utf-8"))
    handler = KafkaJSONDataHandler(context=None, plugin_logger=PluginLogger())  # type: ignore[arg-type]

    messages = list(handler._split_data(data))  # noqa: SLF001

    assert len(messages) == 1
    assert "\\u00f6" not in messages[0].value
    assert "\\u00fc" not in messages[0].value
    assert json.loads(messages[0].value) == {"city": "Köln", "name": "Müller"}


def test_entities_data_handler_split_data_keeps_unicode_characters() -> None:
    """Test that non-ASCII characters in entity values are not escaped"""
    schema = EntitySchema(type_uri="urn:x-test", paths=[EntityPath(path="city")])
    entities = Entities(entities=[Entity(uri="urn:x-1", values=[["Köln"]])], schema=schema)
    handler = KafkaEntitiesDataHandler(context=None, plugin_logger=PluginLogger())  # type: ignore[arg-type]

    messages = list(handler._split_data(entities))  # noqa: SLF001

    assert len(messages) == 1
    assert "\\u00f6" not in messages[0].value
    assert json.loads(messages[0].value)["entity"]["values"]["city"] == ["Köln"]
