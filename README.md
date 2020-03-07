# kafkabat

Kafkabat is simple client to Confluent Schema Registry and Kafka broker,
supports AVRO encoding and decoding. It's inspired by [kafkacat](https://github.com/edenhill/kafkacat)
and [bat](https://github.com/sharkdp/bat).

## Build

```bash
go build
```

## Help

```
$ kafkabat -h
Usage:
    Produce:
        kafkabat KAFKA_OPTS REGISTRY_OPTS --topic=TOPIC [--partition=N] [--key-schema=SCHEMA] [--value-schema=SCHEMA] key value
        kafkabat KAFKA_OPTS REGISTRY_OPTS --topic=TOPIC [--partition=N] [--key-schema=SCHEMA] [--value-schema=SCHEMA] file

            If "file" is "-", read line-by-line JSON objects from stdin, the object must contain keys "key" and "value".

    Consume:
        kafkabat KAFKA_OPTS REGISTRY_OPTS --topic=TOPIC [--partition=N] [--offset=OFFSET] [--count=N] [--follow]

    List:
        kafkabat KAFKA_OPTS [--topic=TOPIC] --list

    KAFKA_OPTS:
        [--broker=BROKERS] [--kafka-version=VERSION]

    REGISTRY_OPTS
        [--registry=URL]


  -b, --broker string          Kafka broker bootstrap servers, separated by comma (default "localhost:9092")
  -t, --topic string           Kafka topic name
  -p, --partition int32        partition, -1 means all (default -1)
  -o, --offset string          offset to start consuming, possile values: begin, end, positive integer, timestamp (default "begin")
  -c, --count uint             maximum count of messages to consume, 0 means no limit
  -r, --registry string        schema regisry URL (default "http://localhost:8081")
  -K, --key-schema string      key schema, can be numeric ID, file path or AVRO schema definition
  -V, --value-schema string    value schema, can be numeric ID, file path or AVRO schema definition
      --kafka-version string   Kafka server version (default "2.3.0")
  -f, --follow                 continue consuming when reach partition end
  -l, --list                   list partition meta information
  -h, --help                   show this help
  -v, --version                show version
```

Check https://github.com/araddon/dateparse for supported time format of
option `--offset`.

## TODO

This tool doesn't support TLS and authentication to schema registry and Kafka broker yet.

