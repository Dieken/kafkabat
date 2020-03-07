package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/araddon/dateparse"
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
	flag.StringVarP(&flags.offset, "offset", "o", "begin", "offset to start consuming, possile values: begin, end, positive integer, timestamp")
	flag.Uint64VarP(&flags.count, "count", "c", 0, "maximum count of messages to consume, 0 means no limit")
	flag.StringVarP(&flags.registry, "registry", "r", "http://localhost:8081", "schema regisry URL")
	flag.StringVarP(&flags.keySchema, "key-schema", "K", "", "key schema, can be numeric ID, file path or AVRO schema definition")
	flag.StringVarP(&flags.valueSchema, "value-schema", "V", "", "value schema, can be numeric ID, file path or AVRO schema definition")
	flag.StringVar(&flags.kafkaVersion, "kafka-version", "2.3.0", "Kafka server version")
	flag.BoolVarP(&flags.follow, "follow", "f", false, "continue consuming when reach partition end")
	flag.BoolVarP(&flags.list, "list", "l", false, "list partition meta information")
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

	if flags.list {
		listTopic(&flags)
		return
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
	count        uint64
	registry     string
	keySchema    string
	valueSchema  string
	kafkaVersion string
	list         bool
	follow       bool
	help         bool
	version      bool
}

func usage() {
	fmt.Fprintln(os.Stderr, `Usage:
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

`)

	flag.PrintDefaults()
}

func listTopic(flags *Flags) {
	var err error

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	if config.Version, err = sarama.ParseKafkaVersion(flags.kafkaVersion); err != nil {
		log.Fatalln("invalid kafka version")
	}

	client, err := sarama.NewClient(strings.Split(flags.brokers, ","), config)
	if err != nil {
		log.Fatalln("failed to create create:", err)
	}
	defer client.Close()

	topics, err := client.Topics()
	if err != nil {
		log.Fatalln("failed to get topics:", err)
	}

	for _, topic := range topics {
		if flags.topic != "" && flags.topic != topic {
			continue
		}

		partitions, err := client.Partitions(topic)
		if err != nil {
			log.Fatalf("failed to list partitions for topic %s: %s\n", topic, err)
		}

		for _, partition := range partitions {
			if flags.partition >= 0 && flags.partition != partition {
				continue
			}

			minOffset, err := client.GetOffset(topic, partition, sarama.OffsetOldest)
			if err != nil {
				log.Fatalf("failed to get oldest offset for topic %s partition %d: %s\n", topic, partition, err)
			}
			maxOffset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
			if err != nil {
				log.Fatalf("failed to get newest offset for topic %s partition %d: %s\n", topic, partition, err)
			}

			if minOffset == maxOffset {
				fmt.Printf("topic=%s partition=%d minOffset=%d maxOffset=%d\n", topic, partition, minOffset, maxOffset)
			} else {
				minMsg, err := getMessageByOffset(client, topic, partition, minOffset)
				if err != nil {
					log.Fatalf("failed to get first message for topic %s partition %d: %s\n", topic, partition, err)
				}
				// due to holes in segment, it's not reliable to obtain previous message
				//maxMsg, err := getMessageByOffset(client, topic, partition, maxOffset-1)
				//if err != nil {
				//	log.Fatalf("failed to get last message for topic %s partition %d: %s\n", topic, partition, err)
				//}
				fmt.Printf("topic=%s partition=%d minOffset=%d maxOffset=%d minTime=%s\n",
					topic, partition, minMsg.Offset, maxOffset, minMsg.Timestamp.Format(time.RFC3339))
			}
		}
	}
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

			jsonKey, jsonValue, ok := getKeyValueFromMap(m)
			if !ok {
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
	var err error
	var count uint64
	var offset int64
	var offsetIsTime bool

	if flags.offset == "begin" {
		offset = sarama.OffsetOldest
	} else if flags.offset == "end" {
		offset = sarama.OffsetNewest
	} else if t, err := dateparse.ParseLocal(flags.offset); err == nil {
		offsetIsTime = true
		offset = t.UnixNano() / 1e6
	} else if offset, err = strconv.ParseInt(flags.offset, 10, 64); err != nil || offset < 0 {
		log.Fatalln("`offset` must be `begin`, `end`, positive integer or timestamp")
	}

	registry := srclient.CreateSchemaRegistryClient(flags.registry)
	keySchema, _ := registry.GetLatestSchema(flags.topic, true)
	valueSchema, _ := registry.GetLatestSchema(flags.topic, false)
	hasKeySchema := keySchema != nil
	hasValueSchema := valueSchema != nil

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	if config.Version, err = sarama.ParseKafkaVersion(flags.kafkaVersion); err != nil {
		log.Fatalln("invalid kafka version")
	}

	client, err := sarama.NewClient(strings.Split(flags.brokers, ","), config)
	if err != nil {
		log.Fatalln("failed to create create:", err)
	}
	defer client.Close()

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Fatalln("failed to create consumer:", err)
	}
	defer consumer.Close()

	partitions, err := client.Partitions(flags.topic)
	if err != nil {
		log.Fatalf("failed to list partitions for topic %s: %s\n", flags.topic, err)
	}

	wg := sync.WaitGroup{}
	lock := sync.Mutex{}
	for _, partition := range partitions {
		if flags.partition >= 0 && flags.partition != partition {
			continue
		}

		newestOffset, err := client.GetOffset(flags.topic, partition, sarama.OffsetNewest)
		if err != nil {
			log.Fatalf("failed to get newest offset for topic %s parition %d: %s\n", flags.topic, partition, err)
		}

		startOffset := offset
		if offsetIsTime {
			startOffset, err = client.GetOffset(flags.topic, partition, startOffset)
			if err != nil {
				log.Fatalf("failed to get offset for topic %s partition %d since %s: %s\n", flags.topic, partition, flags.offset, err)
			}
		}

		if startOffset == sarama.OffsetNewest || startOffset >= newestOffset {
			if !flags.follow {
				continue
			} else {
				startOffset = newestOffset
			}
		}

		wg.Add(1)
		go func(partition int32, newestOffset int64, startOffset int64) {
			defer wg.Done()

			partitionConsumer, err := consumer.ConsumePartition(flags.topic, partition, startOffset)
			if err != nil {
				log.Printf("failed to consume partition %d for topic %s: %s\n", partition, flags.topic, err)
				return
			}
			defer partitionConsumer.Close()

			for {
				select {
				case msg := <-partitionConsumer.Messages():
					var key, value interface{}

					if hasKeySchema {
						key, err = decode(registry, msg.Key)
						if err != nil {
							log.Printf("failed to decode message key, topic=%s partition=%d offset=%d: %s\n", flags.topic, partition, msg.Offset, err)
							break
						}
					} else {
						key = string(msg.Key)
					}

					if hasValueSchema {
						value, err = decode(registry, msg.Value)
						if err != nil {
							log.Printf("failed to decode message value, topic=%s partition=%d offset=%d: %s\n", flags.topic, partition, msg.Offset, err)
							break
						}
					} else {
						value = string(msg.Value)
					}

					m := map[string]interface{}{
						"topic":     flags.topic,
						"partition": partition,
						"offset":    msg.Offset,
						"timestamp": msg.Timestamp,
						"key":       key,
						"value":     value,
					}
					s, err := json.Marshal(m)
					if err != nil {
						log.Printf("failed to serialize to JSON, topic=%s partition=%d offset=%d: %s\n", flags.topic, partition, msg.Offset, err)
						break
					}

					if flags.count > 0 && atomic.AddUint64(&count, 1) > flags.count {
						return
					}
					lock.Lock()
					fmt.Println(string(s))
					lock.Unlock()

					if !flags.follow && msg.Offset >= newestOffset-1 {
						return
					}

				case <-time.After(2 * time.Second):
					if !flags.follow || atomic.LoadUint64(&count) >= flags.count {
						return
					}

				case err := <-partitionConsumer.Errors():
					log.Printf("failed to consume partition %d for topic %s: %s\n", partition, flags.topic, err)
					return
				}
			}
		}(partition, newestOffset, startOffset)
	}

	wg.Wait()
}

func createSchema(registry *srclient.SchemaRegistryClient, schema string, topic string, isKey bool) (*srclient.Schema, error) {
	if schema == "" {
		s, err := registry.GetLatestSchema(topic, isKey)
		if err != nil && strings.HasPrefix(err.Error(), "404 Not Found") {
			return nil, nil
		}
		return s, err
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
		log.Printf("failed to send, err=%s, msg=%s\n", err.Error(), s)
		return false
	}

	*successes++
	log.Printf("[%d] partition=%d, offset=%d, msg=%s\n", *successes, partition, offset, s)
	return true
}

func getFieldFromMap(m map[string]interface{}, k1 string, k2 string) (interface{}, bool) {
	var value interface{}
	var ok bool
	if value, ok = m[k1]; !ok {
		if value, ok = m[k2]; !ok {
			log.Printf("no `%s` or `%s` field found in object %s\n", k1, k2, m)
			return nil, false
		}
	}
	if value == nil {
		log.Printf("skip null %s in object %s\n", k2, m)
		return nil, false
	}

	return value, true
}

func getKeyValueFromMap(m map[string]interface{}) (interface{}, interface{}, bool) {
	key, ok1 := getFieldFromMap(m, "Key", "key")
	value, ok2 := getFieldFromMap(m, "Value", "value")
	return key, value, ok1 && ok2
}

func getMessageByOffset(client sarama.Client, topic string, partition int32, offset int64) (*sarama.ConsumerMessage, error) {
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, err
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		return nil, err
	}
	defer partitionConsumer.Close()

	select {
	case msg := <-partitionConsumer.Messages():
		return msg, nil
	case err := <-partitionConsumer.Errors():
		return nil, err
	}
}

func decode(registry *srclient.SchemaRegistryClient, msg []byte) (interface{}, error) {
	if msg == nil || len(msg) < 6 {
		return nil, errors.New("invalid message")
	}

	schemaID := binary.BigEndian.Uint32(msg[1:5])
	schema, err := registry.GetSchema(int(schemaID))
	if err != nil {
		return nil, err
	}

	datum, _, err := schema.Codec().NativeFromBinary(msg[5:])
	return datum, err
}
