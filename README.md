# cmem-plugin-kafka

This eccenca Corporate Memory plugin allows for sending and receiving messages from Apache Kafka.

## Installation

`cmemc -c my-cmem admin workspace python install cmem-plugin-kafka`

## Development

- Run [task](https://taskfile.dev/) to see all major development tasks
- Use [pre-commit](https://pre-commit.com/) to avoid errors before commit
- This repository was created with [this](https://github.com/eccenca/cmem-plugin-template) [copier](https://copier.readthedocs.io/) template.

### Running Test

This plugin needs needs running Kafka and Corporate Memory orchestrations:

In order to setup access to your Corporate Memory uses [cmemc](https://eccenca.com/go/cmemc)'s config eval command to fill environment variables:
```shell-session
$ eval $(cmemc -c my-cmem config eval)
```

In order to setup access to your Kafka, write the connection details to the `.env` file:
```shell-session
$ cat .env
KAFKA_BOOTSTRAP_SERVER=localhost:9092
KAFKA_SECURITY_PROTOCOL=PLAINTEXT
```

To run a Kafka orchestration locally, you can use task:
```shell-session
$ task custom:kafka:start
task: [custom:kafka:start] docker-compose -f docker/docker-compose.yml up --wait --no-color --force-recreate --renew-anon-volumes

[+] Running 2/2
 ⠿ Container docker-zookeeper-1  Healthy                                          1.1s
 ⠿ Container docker-kafka-1      Healthy                                          1.1s
```

Having Kafka as well as Corporate Memory in place, run the test suite with `task check`.

### confluent-python installation

#### ARM based Macs

* make sure `brew doctor` has no issues
* install with `brew install librdkafka`
* Provide this in your environment (based on [this answer](https://apple.stackexchange.com/questions/414622/installing-a-c-c-library-with-homebrew-on-m1-macs))
```
export CPATH=/opt/homebrew/include
export LIBRARY_PATH=/opt/homebrew/lib
```
* test build from source in separate environment with:
  * `pip install https://files.pythonhosted.org/packages/fb/16/d04dded73439266a3dbcd585f1128483dcf509e039bacd93642ac5de97d4/confluent-kafka-1.8.2.tar.gz`
* then try `poetry install`

