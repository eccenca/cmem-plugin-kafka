import json

from cmem_plugin_kafka.utils import get_kafka_statistics


def test_get_kafka_statistics():
    input = {
        "name": "producer",
        "client_id": "producer-kafka-testing",
        "type": "C",
        "time": "10212",
        "msg_cnt": "10",
        "msg_size": "1000",
        "topics": ["eccenca"],
        "extra_key": "no_interested",
    }
    filtered_output = get_kafka_statistics(json.dumps(input))
    assert filtered_output.get("extra_key") is None
    assert input["topics"][0] == filtered_output["topics"]

    del input["extra_key"]
    del input["topics"]
    del filtered_output["topics"]

    assert filtered_output == input
