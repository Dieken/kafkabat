package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/riferrei/srclient"
	flag "github.com/spf13/pflag"
)

const VERSION = "kafkabat v0.1.0"

func main() {
	log.SetOutput(os.Stderr)
	log.SetFlags(log.Lmicroseconds)

	flags := Flags{}

	flag.CommandLine.SortFlags = false
	flag.Usage = usage
	flag.StringVarP(&flags.brokers, "broker", "b", "localhost:9092", "Kafka broker bootstrap servers, separated by comma")
	flag.StringVarP(&flags.topic, "topic", "t", "", "Kafka topic name")
	flag.Int32VarP(&flags.partition, "partition", "p", -1, "partition, -1 means all")
	flag.StringVarP(&flags.offset, "offset", "o", "end", "offset to start consuming")
	flag.StringVarP(&flags.registry, "registry", "r", "http://localhost:8081", "schema regisry URL")
	flag.StringVarP(&flags.keySchema, "key-schema", "K", "", "key schema, can be numeric ID, file path or AVRO schema definition")
	flag.StringVarP(&flags.valueSchema, "value-schema", "V", "", "value schema, can be numeric ID, file path or AVRO schema definition")
	flag.StringVar(&flags.kafkaVersion, "kafka-version", "2.3.0", "Kafka server version")
	flag.BoolVarP(&flags.help, "help", "h", false, "show this help")
	flag.BoolVarP(&flags.version, "version", "v", false, "show version")

	flag.Parse()

	if flags.version {
		fmt.Println(VERSION)
		return
	}

	if flags.help {
		usage()
		os.Exit(1)
	}

	if flags.topic == "" {
		fmt.Fprintln(os.Stderr, "ERROR: `topic` isn't specified!")
		usage()
		os.Exit(1)
	}

	if flag.NArg() > 0 {
		runProducer(&flags)
	} else {
		runConsumer(&flags)
	}
}

type Flags struct {
	brokers      string
	topic        string
	partition    int32
	offset       string
	registry     string
	keySchema    string
	valueSchema  string
	kafkaVersion string
	help         bool
	version      bool
}

func usage() {
	fmt.Fprintln(os.Stderr, `Usage:
    Producer:
        kafkabat KAFKA_OPTS REGISTRY_OPTS key value
        kafkabat KAFKA_OPTS REGISTRY_OPTS [json_stream_file | -]

    Consumer:
        kafkabat KAFKA_OPTS REGISTRY_OPTS [--offset=OFFSET]

    KAFKA_OPTS:
        [--broker=BROKERS] --topic=TOPIC [--partition=N] [--kafka-version=VERSION]

    REGISTRY_OPTS
        [--registry=URL] [--key-schema=SCHEMA] [--value-schema=SCHEMA]

`)

	flag.PrintDefaults()
}

func runProducer(flags *Flags) {
	registry := srclient.CreateSchemaRegistryClient(flags.registry)

	keySchema, err := createSchema(registry, flags.keySchema, flags.topic, true)
	if err != nil {
		log.Fatalln("invalid key schema:", err)
	}
	valueSchema, err := createSchema(registry, flags.valueSchema, flags.topic, false)
	if err != nil {
		log.Fatalln("invalid value schema:", err)
	}

	config := sarama.NewConfig()
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Idempotent = true
	config.Net.MaxOpenRequests = 1
	if flags.partition >= 0 {
		config.Producer.Partitioner = sarama.NewManualPartitioner
	}
	if config.Version, err = sarama.ParseKafkaVersion(flags.kafkaVersion); err != nil {
		log.Fatalln("invalid kafka version")
	}

	client, err := sarama.NewClient(strings.Split(flags.brokers, ","), config)
	if err != nil {
		log.Fatalln("failed to create client:", err)
	}
	defer client.Close()

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		log.Fatalln("failed to create producer:", err)
	}
	defer producer.Close()

	done := false
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		signal := <-signals
		log.Println("got signal:", signal)
		done = true
	}()

	successes := 0
	if flag.NArg() > 1 {
		argKey, argValue := flag.Arg(0), flag.Arg(1)

		key, err := str2Avro(keySchema, argKey)
		if err != nil {
			log.Fatalf("failed to encode key `%s`: %s", argKey, err)
		}

		value, err := str2Avro(valueSchema, argValue)
		if err != nil {
			log.Fatalf("failed to encode value `%s`: %s", argValue, err)
		}

		sendMessage(producer, flags.topic, flags.partition, key, value, &successes)
	} else {
		filename := flag.Arg(0)
		f := os.Stdin
		if filename != "-" {
			if f, err = os.Open(filename); err != nil {
				log.Fatalf("failed to open %s: %s", filename, err)
			}
		}

		jsonDecoder := json.NewDecoder(f)
		for {
			var m map[string]interface{}
			if err := jsonDecoder.Decode(&m); err == io.EOF {
				break
			} else if err != nil {
				log.Fatal(err)
			}

			var jsonKey, jsonValue interface{}
			var ok bool
			if jsonKey, ok = m["Key"]; !ok {
				if jsonKey, ok = m["key"]; !ok {
					log.Println("no `Key` or `key` field found in object", m)
					continue
				}
			}
			if jsonKey == nil {
				log.Println("skip null key for object", m)
				continue
			}
			if jsonValue, ok = m["Value"]; !ok {
				if jsonValue, ok = m["value"]; !ok {
					log.Println("no `Value` or `value` field found in object", m)
					continue
				}
			}
			if jsonValue == nil {
				log.Println("skip null value for object", m)
				continue
			}

			key, err := json2Avro(keySchema, jsonKey)
			if err != nil {
				log.Fatalf("failed to encode key `%v`: %s", jsonKey, err)
			}

			value, err := json2Avro(valueSchema, jsonValue)
			if err != nil {
				log.Fatalf("failed to encode value `%v`: %s", jsonValue, err)
			}

			if done || !sendMessage(producer, flags.topic, flags.partition, key, value, &successes) {
				break
			}
		}
	}
}

func runConsumer(flags *Flags) {
}

func createSchema(registry *srclient.SchemaRegistryClient, schema string, topic string, isKey bool) (*srclient.Schema, error) {
	if schema == "" {
		return nil, nil
	}

	if _, err := os.Stat(schema); os.IsNotExist(err) {
		if id, err := strconv.Atoi(schema); err != nil {
			return registry.CreateSchema(topic, schema, isKey)
		} else {
			return registry.GetSchema(id)
		}
	}

	content, err := ioutil.ReadFile(schema)
	if err != nil {
		return nil, err
	}
	return registry.CreateSchema(topic, string(content), isKey)
}

func encode(schema *srclient.Schema, datum interface{}) (sarama.ByteEncoder, error) {
	buffer := make([]byte, 5, 256)
	buffer[0] = 0
	binary.BigEndian.PutUint32(buffer[1:5], uint32(schema.ID()))
	if bytes, err := schema.Codec().BinaryFromNative(buffer, datum); err != nil {
		return nil, err
	} else {
		return sarama.ByteEncoder(bytes), nil
	}
}

func str2Avro(schema *srclient.Schema, s string) (sarama.Encoder, error) {
	if schema == nil {
		return sarama.StringEncoder(s), nil
	}

	var obj interface{}
	if err := json.Unmarshal([]byte(s), &obj); err != nil {
		return nil, err
	}

	return encode(schema, obj)
}

func json2Avro(schema *srclient.Schema, obj interface{}) (sarama.Encoder, error) {
	if schema == nil {
		if s, ok := obj.(string); ok {
			return sarama.StringEncoder(s), nil
		}
		if bytes, err := json.Marshal(obj); err != nil {
			return nil, err
		} else {
			return sarama.ByteEncoder(bytes), nil
		}
	}

	return encode(schema, obj)
}

func sendMessage(producer sarama.SyncProducer, topic string, partition int32, key sarama.Encoder, value sarama.Encoder, successes *int) bool {
	msg := sarama.ProducerMessage{Topic: topic, Partition: partition, Key: key, Value: value, Timestamp: time.Now()}
	partition, offset, err := producer.SendMessage(&msg)
	s, _ := json.Marshal(msg)
	if err != nil {
		log.Printf("failed to send, err=%s msg=%s\n", err.Error(), s)
		return false
	}

	*successes++
	log.Printf("[%d] partition=%d, offset=%d, msg=%s\n", *successes, partition, offset, s)
	return true
}
