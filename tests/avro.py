from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

config = {
    "bootstrap.servers": "172.17.0.1:9092",
    "security.protocol": "PLAINTEXT"
}


if __name__ == '__main__':
    _producer = Producer(config)
    _producer.produce(
        "self._topic",
        value="SAI",
        key="123",
    )
    print("SAIPRANEEY")
