package repeater

import (
  "log"
  "time"
  "github.com/confluentinc/confluent-kafka-go/kafka"
)

// TopicProducer is a wrapper of the librdkafka producer.
//
// It's holding a kafka ConfigMap annd a kafka producer
//
// Order is to indicates the message should be delivered successfully to kafka before the next one
//
// 'toChannel' specified if to use producer Events to receive kafka delivery events
//
// 'delivery' is the golang channel to receive the kafka delivery event
type TopicProducer struct {
  Topic      string
  config     *kafka.ConfigMap
  producer   *kafka.Producer
  Order      bool

  delivery   chan kafka.Event
  toChannel  bool
}

// TopicConsumer is wrapper of librdkafka consumer, it's holding a kafka consumer and a kafka ConfigMap
type TopicConsumer struct {
  Topic      string
  config     *kafka.ConfigMap
  consumer   *kafka.Consumer
}

// When order is off, the message would be delivered without blocking
func (p *TopicProducer) produce(data []byte) {
  msg := &kafka.Message {
    TopicPartition: kafka.TopicPartition{ Topic: &p.Topic, Partition: kafka.PartitionAny },
    Value: data,
    Headers: []kafka.Header{},
  }

  if p.toChannel {
    p.producer.ProduceChannel() <- msg
  } else {
    err := p.producer.Produce(msg, p.delivery)
    if err != nil {
      log.Printf("Failed to enqueue the message: %v : %v, will retry in one minute", string(data), err)
      time.Sleep(60 * time.Second)
    }
  }
}

// Setup the producer with configurations
func (p *TopicProducer) Setup(config *Config) {
  var extra map[string]interface{}
  p.Topic, p.config, extra = parseKafkaConfig(config)

  toChannel, ok := extra["to_channel"]
  if ok {
    p.toChannel = toChannel.(bool)
  }

  order, ok := extra["order"]
  if ok {
    p.Order = order.(bool)
  }

  producer, err := kafka.NewProducer(p.config)
  if err != nil {
    log.Fatalf("Failed to create producer: %v", err)
  }
  p.producer = producer

  if !p.Order {
    if p.toChannel {
      log.Println("Using unordered channel producer")
      go func () {
        for e:= range p.producer.Events() {
          p.handleEvent(e)
        }
      }()
    } else {
      log.Println("Using unordered producer")
      p.delivery = make(chan kafka.Event, 200)
      go func() {
        for {
          e := <-p.delivery
          p.handleEvent(e)
        }
      } ()
    }
  }
}

// Process is the entry function to handle the task of delivery a message to kakfa
//
// When 'order' is true, it will block until it received the delivery success event
func (p *TopicProducer) Process(data []byte) {
  //TODO: This is a blocking way to wrap a non-blocking producer
  // When FIFO is not required, a pential speed up here
  if !p.Order {
    p.produce(data)
    return
  }

  for {
    var success bool
    if p.toChannel {
      success = p.process(data)
    } else {
      success = p.processAlt(data)
    }
    if success {
      return
    } else {
      log.Println("Failed to write message to kafka, will retry again")
      time.Sleep(time.Second)
    }
  }
}

// Handler of kafka delivery event, when the delivery failed, a new delivery will be initalized.
func (p *TopicProducer) handleEvent(event kafka.Event) {
  switch ev := event.(type) {
  case *kafka.Message:
    m := ev
    if m.TopicPartition.Error != nil {
      log.Printf("Delivery failed: %v message: %v", m.TopicPartition.Error, string(m.Value))
      p.produce(m.Value)
    } else {
      log.Printf("Produced message %v", string(m.Value))
    }
  }
}

// A blocking delivery of kafka message using kafka ProduceChannel for queuing
func (p *TopicProducer) process(data []byte) bool {
  sig := make(chan bool)
  defer close(sig)
  go func () {
    for e:= range p.producer.Events() {
      switch ev := e.(type) {
      case *kafka.Message:
        m := ev
        if m.TopicPartition.Error != nil {
          log.Printf("Delivery failed: %v", m.TopicPartition.Error)
          sig <- false
        } else {
          log.Printf("Produced message %v", string(m.Value))
          sig <- true
        }
        return
      }
    }
  }()

  p.producer.ProduceChannel() <-&kafka.Message{
    TopicPartition: kafka.TopicPartition{ Topic: &p.Topic, Partition: kafka.PartitionAny },
    Value: data,
    Headers: []kafka.Header{},
  }
  return <-sig
}

// A blocking delivery of kafka message with custo golang channel to receive kafka event
func (p *TopicProducer) processAlt(data []byte) bool {
  delivery := make(chan kafka.Event)
  defer close(delivery)
  err := p.producer.Produce(&kafka.Message{
    TopicPartition: kafka.TopicPartition{ Topic: &p.Topic, Partition: kafka.PartitionAny },
    Value: data,
    Headers: []kafka.Header{},
  }, delivery)
  if err != nil {
    log.Println(err)
    log.Printf("Can not enque message: %v, %v", string(data), err)
    return false
  }
  e := <-delivery
  m := e.(*kafka.Message)
  if m.TopicPartition.Error != nil {
    log.Printf("Delivery failed: %v", m.TopicPartition.Error)
    return false
  }
  log.Printf("Produced message %v", string(m.Value))
  return true
}

// Setup a TopicConsunmer with kafka configurations, and subscribe to topics
func (c *TopicConsumer) Setup(config *Config) {
  c.Topic, c.config, _ = parseKafkaConfig(config)
  consumer, err := kafka.NewConsumer(c.config)
  if err != nil {
    log.Fatalf("Failed to create consumer: %v", err)
  }
  c.consumer = consumer
  log.Printf("Subscribing to %v", c.Topic)
  c.consumer.SubscribeTopics([]string{c.Topic}, nil)
}

// Poll would call the kafka consumer's Poll to retrieve a kafka message
func (c *TopicConsumer) Poll(n int) *kafka.Message {
  ev := c.consumer.Poll(n)
  switch e:= ev.(type) {
  case *kafka.Message:
    log.Printf("Consumed message: %v", string(e.Value))
    return e
  case kafka.Error:
    log.Printf("%% Error: %v: %v", e.Code(), e)
    if e.Code() == kafka.ErrAllBrokersDown {
      log.Fatalf("Kafka consumer is stopping for kafka brokers down!")
    }
    /*
  default:
    log.Printf("Ignored %v", e)
    */
  }
  return nil
}

// Parser of kafka configuration, it will convert the json configuration to valid kafka configurations
func parseKafkaConfig (config *Config) (string, *kafka.ConfigMap, map[string]interface{}) {
  var c map[string] interface {}
  if config.Topic != nil {
    c = config.Topic
  } else {
    c = make(map[string]interface{})
  }

  extra := make(map[string]interface{})

  topic, ok := c["topic"]
  if !ok {
      log.Fatalln("A valid topic name needs to be provided")
  }

  order, ok := c["order"]
  if ok {
    extra["order"] = order.(bool)
  }

  toChannel, ok := c["to_channel"]
  if ok {
    extra["to_channel"] = toChannel.(bool)
  }

  params, ok := c["params"]
  var paramSet map[string]interface{}
  if ok {
    paramSet = params.(map[string]interface{})
  }

  configMap := &kafka.ConfigMap {}
  for k, v := range paramSet {
    switch value := v.(type) {
    case float64:
      configMap.SetKey(k, int(value))
    default:
      configMap.SetKey(k, value)
    }
  }

  return topic.(string), configMap, extra
}

// Create a TopicProducer instance and set it up.
func NewTopicProducer(config *Config) *TopicProducer {
  producer := &TopicProducer { toChannel: true, Order: true }
  producer.Setup(config)

  return producer
}

// Create a TopicConsumer instance and set it up
func NewTopicConsumer(config *Config) *TopicConsumer {
  consumer := &TopicConsumer {}
  consumer.Setup(config)

  return consumer
}


