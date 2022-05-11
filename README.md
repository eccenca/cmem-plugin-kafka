# cmem-plugin-siekafka

Producer / consumer plugins for SIEPKN

## Development

- Run [task](https://taskfile.dev/) to see all major development tasks
- Use [pre-commit](https://pre-commit.com/) to avoid errors before commit
- This repository was created with [this]() [copier](https://copier.readthedocs.io/) template.

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

