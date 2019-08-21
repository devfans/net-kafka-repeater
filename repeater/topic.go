package repeater

import (
  "log"
)


type TopicProducer struct {
  Topic      string
  params     map[string] interface {}
}

type TopicConsumer struct {
  Topic      string
  params     map[string] interface {}
}

func NewTopicProducer(config *Config) *TopicProducer {
  producer := &TopicProducer {}

  var c map[string] interface {}
  if config.Topic != nil {
    c = config.Topic
  } else {
    c = make(map[string]interface{})
  }

  topic, ok := c["topic"]
  if ok {
    producer.Topic = topic.(string)
  } else {
      log.Fatalln("A valid topic name needs to be provided")
  }
  return producer
}

func NewTopicConsumer(config *Config) *TopicConsumer {
  consumer := &TopicConsumer {}

  var c map[string] interface {}
  if config.Topic != nil {
    c = config.Topic
  } else {
    c = make(map[string]interface{})
  }

  topic, ok := c["topic"]
  if ok {
    consumer.Topic = topic.(string)
  } else {
    log.Fatalln("A valid topic name needs to be provided")
  }
  return consumer
}
