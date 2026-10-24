package kafka

import kafkago "github.com/segmentio/kafka-go"

type kafkaMessage = kafkago.Message

func kafkaMsg(value []byte) kafkago.Message {
	return kafkago.Message{Topic: ProductMasterTopic, Value: value}
}
