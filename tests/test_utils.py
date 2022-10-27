""" Test utils """
import json

from cmem_plugin_kafka.utils import get_kafka_statistics


def test_get_kafka_statistics():
    """test kafka statistics"""
    json_input = {
        "name": "producer",
        "client_id": "producer-kafka-testing",
        "type": "C",
        "time": "10212",
        "msg_cnt": "10",
        "msg_size": "1000",
        "topics": ["eccenca"],
        "extra_key": "no_interested",
    }
    filtered_output = get_kafka_statistics(json.dumps(json_input))
    assert filtered_output.get("extra_key") is None
    assert json_input["topics"][0] == filtered_output["topics"]

    del json_input["extra_key"]
    del json_input["topics"]
    del filtered_output["topics"]

    assert filtered_output == json_input
