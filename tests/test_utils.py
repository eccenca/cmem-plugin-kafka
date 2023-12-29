"""Tests for `utils` package."""
import json

from cmem_plugin_kafka.utils import get_kafka_statistics


def test_get_kafka_statistics() -> None:
    """Test the get_kafka_statistics"""
    _input = {
        "name": "producer",
        "client_id": "producer-kafka-testing",
        "type": "C",
        "time": "10212",
        "msg_cnt": "10",
        "msg_size": "1000",
        "topics": {"eccenca": {}},
        "extra_key": "no_interested",
    }
    filtered_output = get_kafka_statistics(json.dumps(_input))
    assert filtered_output.get("extra_key") is None
    assert ",".join(_input["topics"].keys()) == filtered_output["topics"]

    del _input["extra_key"]
    del _input["topics"]
    del filtered_output["topics"]

    assert filtered_output == _input
