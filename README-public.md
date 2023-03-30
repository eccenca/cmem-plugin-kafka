# cmem-plugin-kafka

Send and receive messages from Apache Kafka.

This is a plugin for [eccenca](https://eccenca.com) [Corporate Memory](https://documentation.eccenca.com).

You can install it with the [cmemc](https://eccenca.com/go/cmemc) command line
clients like this:

```
cmemc admin workspace python install cmem-plugin-kafka
```

## plugin supported message format

### XML dataset format
```
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

### JSON Dataset format
```
[
  {
    "message": {
      "key": "818432-942813-832642-453478",
      "content": {
        "location": [
          "Leipzig"
        ],
        "obstacle": {
          "name": "Iron Bars",
          "order": "1"
        }
      }
    },
    "headers": {
      "type": "ADD"
    }
  },
  {
    "message": {
      "key": "887428-119918-570674-866526",
      "content": {
        "comments": "We can pass any json payload here."
      }
    },
    "headers": {
      "type": "REMOVE"
    }
  }
]
```
### Entities format
```
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