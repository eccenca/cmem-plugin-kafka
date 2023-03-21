import time

import pytest


@pytest.fixture
def kafka_service(module_scoped_container_getter) -> str:
    """Wait for the api from kafka to become responsive"""
    broker = module_scoped_container_getter.get("kafka")
    service = broker.network_info[0]
    time.sleep(10)
    base_url = f"{service.hostname}:{service.host_port}"
    return base_url
