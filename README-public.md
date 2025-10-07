# cmem-plugin-kafka

Send and receive messages from Apache Kafka.

[![eccenca Corporate Memory][cmem-shield]][cmem-link]

This is a plugin for [eccenca](https://eccenca.com) [Corporate Memory](https://documentation.eccenca.com). You can install it with the [cmemc](https://eccenca.com/go/cmemc) command line client like this:

```
cmemc admin workspace python install cmem-plugin-kafka
```
[![workflow](https://github.com/eccenca/cmem-plugin-kafka/actions/workflows/check.yml/badge.svg)](https://github.com/eccenca/cmem-plugin-kafka/actions) [![pypi version](https://img.shields.io/pypi/v/cmem-plugin-kafka)](https://pypi.org/project/cmem-plugin-kafka) [![license](https://img.shields.io/pypi/l/cmem-plugin-kafka)](https://pypi.org/project/cmem-plugin-kafka)
[![poetry][poetry-shield]][poetry-link] [![ruff][ruff-shield]][ruff-link] [![mypy][mypy-shield]][mypy-link] [![copier][copier-shield]][copier] 

[cmem-link]: https://documentation.eccenca.com
[cmem-shield]: https://img.shields.io/endpoint?url=https://dev.documentation.eccenca.com/badge.json
[poetry-link]: https://python-poetry.org/
[poetry-shield]: https://img.shields.io/endpoint?url=https://python-poetry.org/badge/v0.json
[ruff-link]: https://docs.astral.sh/ruff/
[ruff-shield]: https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json&label=Code%20Style
[mypy-link]: https://mypy-lang.org/
[mypy-shield]: https://www.mypy-lang.org/static/mypy_badge.svg
[copier]: https://copier.readthedocs.io/
[copier-shield]: https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/copier-org/copier/master/img/badge/badge-grayscale-inverted-border-purple.json

## Plugin supported message format

### XML dataset format

An example XML document is shown below. This document will be sent as two messages
to the configured topic. Each message is created as a proper XML document.

```xml
<?xml version="1.0" encoding="utf-8"?>
<KafkaMessages>
    <Message>
        <PurchaseOrder OrderDate="1996-04-06">
            <ShipTo country="string">
                <name>string</name>
                <street>string</street>
                <city>string</city>
                <state>string</state>
                <zip>9200</zip>
            </ShipTo>
            <BillTo country="string">
                <name>string</name>
                <street>string</street>
                <city>string</city>
                <state>string</state>
                <zip>2381</zip>
            </BillTo>
        </PurchaseOrder>
    </Message>
    <Message key="1234">
        <SingleTagHere>
            .
            .
            .
        </SingleTagHere>
    </Message>
</KafkaMessages>
```
Producer plugin generates 2 messages with below content
```xml
<?xml version="1.0" encoding="utf-8"?>
<PurchaseOrder OrderDate="1996-04-06">
    <ShipTo country="string">
        <name>string</name>
        <street>string</street>
        <city>string</city>
        <state>string</state>
        <zip>9200</zip>
    </ShipTo>
    <BillTo country="string">
        <name>string</name>
        <street>string</street>
        <city>string</city>
        <state>string</state>
        <zip>2381</zip>
    </BillTo>
</PurchaseOrder>
```
```xml
<?xml version="1.0" encoding="utf-8"?>
<SingleTagHere>
            .
            .
            .
</SingleTagHere>
```
### JSON Dataset format

An example JSON document is shown below. This document will be sent as two messages
to the configured topic. Each message is created as a proper JSON document.

```json
[
  {
    "message": {
      "key": "818432-942813-832642-453478",
      "headers": {
        "type": "ADD"
      },
      "content": {
        "location": [
          "Leipzig"
        ],
        "obstacle": {
          "name": "Iron Bars",
          "order": "1"
        }
      }
    }
  },
  {
    "message": {
      "key": "887428-119918-570674-866526",
      "headers": {
        "type": "REMOVE"
      },
      "content": {
        "comments": "We can pass any json payload here."
      }
    }
  }
]
```
Producer plugin generates 2 messages with below content
```json
{
  "location": [
    "Leipzig"
  ],
  "obstacle": {
    "name": "Iron Bars",
    "order": "1"
  }
}
```
```json
{
  "comments": "We can pass any json payload here."
}
```
### Entities format

Random values plugin entities will generate below format JSON document.

```json
{
  "schema": {
    "type_uri": "https://example.org/vocab/RandomValueRow"
  },
  "entity": {
    "uri": "urn:uuid:3c68d8e7-bf17-4045-a9eb-c9c9813f717f",
    "values": {
      "<https://example.org/vocab/RandomValuePath0>": [
        "a8o4Ocsb6RZClFRUZU3b2w"
      ],
      "<https://example.org/vocab/RandomValuePath1>": [
        "RTICRU7JcTUVn94decelPg"
      ],
      "<https://example.org/vocab/RandomValuePath2>": [
        "A9r-969NjAlX0DNWftxKoA"
      ],
      "<https://example.org/vocab/RandomValuePath3>": [
        "FygWRy1UJ4-IzIim1qukJA"
      ],
      "<https://example.org/vocab/RandomValuePath4>": [
        "AJcbn-LJEs-Dif96xu2eww"
      ]
    }
  }
}
```