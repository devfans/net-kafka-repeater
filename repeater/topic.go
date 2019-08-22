package repeater

import (
  "log"
  "time"
  "github.com/confluentinc/confluent-kafka-go/kafka"
)


type TopicProducer struct {
  Topic      string
  params     map[string] interface {}
  config     *kafka.ConfigMap
  producer   *kafka.Producer
}

type TopicConsumer struct {
  Topic      string
  params     map[string] interface {}
  config     *kafka.ConfigMap
  consumer   *kafka.Consumer
}

func (p *TopicProducer) Setup(config *Config) {
  p.Topic, p.config = parseKafkaConfig(config)
  producer, err := kafka.NewProducer(p.config)
  if err != nil {
    log.Fatalf("Failed to create producer: %v", err)
  }
  p.producer = producer
}

func (p *TopicProducer) Process(data []byte) {
  //TODO: This is a blocking way to wrap a non-blocking producer
  // When FIFO is not required, a pential speed up here
  for {
    if p.process(data) {
      return
    } else {
      log.Println("Failed to write message to kafka, will retry again")
      time.Sleep(time.Second)
    }
  }
}

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

func (p *TopicProducer) processFun(data []byte) bool {
  delivery := make(chan kafka.Event)
  err := p.producer.Produce(&kafka.Message{
    TopicPartition: kafka.TopicPartition{ Topic: &p.Topic, Partition: kafka.PartitionAny },
    Value: data,
    Headers: []kafka.Header{},
  }, delivery)
  if err != nil {
    log.Println(err)
  }
  e := <-delivery
  close(delivery)
  m := e.(*kafka.Message)
  if m.TopicPartition.Error != nil {
    log.Printf("Delivery failed: %v", m.TopicPartition.Error)
    return false
  }
  log.Printf("Produced message %v", string(m.Value))
  return true
}

func (c *TopicConsumer) Setup(config *Config) {
  c.Topic, c.config = parseKafkaConfig(config)
  consumer, err := kafka.NewConsumer(c.config)
  if err != nil {
    log.Fatalf("Failed to create consumer: %v", err)
  }
  c.consumer = consumer
  log.Printf("Subscribing to %v", c.Topic)
  c.consumer.SubscribeTopics([]string{c.Topic}, nil)
}

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

func parseKafkaConfig (config *Config) (string, *kafka.ConfigMap) {
  var c map[string] interface {}
  if config.Topic != nil {
    c = config.Topic
  } else {
    c = make(map[string]interface{})
  }

  topic, ok := c["topic"]
  if !ok {
      log.Fatalln("A valid topic name needs to be provided")
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

  return topic.(string), configMap
}

func NewTopicProducer(config *Config) *TopicProducer {
  producer := &TopicProducer {}
  producer.Setup(config)

  return producer
}

func NewTopicConsumer(config *Config) *TopicConsumer {
  consumer := &TopicConsumer {}
  consumer.Setup(config)

  return consumer
}


