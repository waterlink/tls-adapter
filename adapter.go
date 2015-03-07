package main

import (
	"github.com/Shopify/sarama"
	"github.com/huin/mqtt"
	"log"
	"net"
)

const kafkaPort = 9092

func obtainListener(port string) (net.Listener, error) {
	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	return listener, nil
}

func qosFromTopics(topics []mqtt.TopicQos) (topicsQos []mqtt.QosLevel) {
	topicsQos = make([]mqtt.QosLevel, len(topics))
	for index, topic := range topics {
		topicsQos[index] = topic.Qos
	}
	return topicsQos
}

func handleClient(conn net.Conn, kafkaBrokers []string, kafkaConfig *sarama.Config) {
	topics := make(map[string]*sarama.PartitionConsumer)

	consumer, err := sarama.NewConsumer(kafkaBrokers, kafkaConfig)
	if err != nil {
		log.Print(err)
		return
	}

	//producer, err := sarama.NewConsumer(kafkaBrokers, kafkaConfig)
	//if err != nil {
	//	log.Print(err)
	//	return
	//}

	defer conn.Close()
	defer consumer.Close()
	//defer producer.Close()

	for {
		msg, err := mqtt.DecodeOneMessage(conn, nil)
		if err != nil {
			log.Print(err)
			break
		}

		switch message := msg.(type) {

		case *mqtt.Connect:
			log.Print("got Connect, gonna send ConnAck")
			msg := &mqtt.ConnAck{
				Header: mqtt.Header{
					DupFlag:  false,
					QosLevel: mqtt.QosAtLeastOnce,
					Retain:   false,
				},
				ReturnCode: mqtt.ReturnCode(mqtt.RetCodeAccepted),
			}
			if err := msg.Encode(conn); err != nil {
				log.Print(err)
			}

		case *mqtt.Subscribe:
			log.Print("got Subscribe, gonna send SubAck")
			log.Print(message)
			msg := &mqtt.SubAck{
				Header:    message.Header,
				MessageId: message.MessageId,
				TopicsQos: qosFromTopics(message.Topics),
			}
			log.Print(msg)
			if err := msg.Encode(conn); err != nil {
				log.Print(err)
			}

			for _, topic := range message.Topics {
				topicConsumer, err := consumer.ConsumePartition(topic.Topic, 0, 0)
				if err != nil {
					log.Printf("unable to consume topic %s\n", topic.Topic)
					log.Print(err)
					topicConsumerRetry, err := consumer.ConsumePartition(topic.Topic, 0, 0)
					if err != nil {
						log.Printf("unable to consume topic %s\n", topic.Topic)
						log.Print(err)
					}
					topicConsumer = topicConsumerRetry
				}
				topics[topic.Topic] = topicConsumer
				if topicConsumer != nil {
					defer topicConsumer.Close()
				}
			}

			for topicName, topic := range topics {
				if topic != nil {
					go func() {
						messages := topic.Messages()

						for {
							kafkaMessage, ok := <-messages
							if !ok {
								break
							}

							log.Print(string(kafkaMessage.Value))
							dataToSend := string(kafkaMessage.Value)
							msg2 := &mqtt.Publish{
								Header:    message.Header,
								TopicName: topicName,
								MessageId: 10,
								Payload:   mqtt.BytesPayload(dataToSend),
							}
							if err := msg2.Encode(conn); err != nil {
								log.Print(err)
								break
							}
						}
					}()

					go func() {
						errors := topic.Errors()

						for {
							error, ok := <-errors
							if !ok {
								break
							}

							log.Print(error)
						}
					}()
				}
			}

		case *mqtt.Disconnect:
			log.Print("got Disconnect, closing connection")
			log.Print(message)
			break

		case *mqtt.PingReq:
			log.Print("got PingReq, gonna send PingResp")
			log.Print(message)
			msg := &mqtt.PingResp{
				Header: message.Header,
			}
			if err := msg.Encode(conn); err != nil {
				log.Print(err)
			}

		case *mqtt.Publish:
			log.Print("got Publish, gonna send to Kafka and respond with PubAck")
			log.Print(message)
			//kafkaMessage := &ProducerMessage{
			//	Topic: message.Topic,
			//
			//}

		case *mqtt.PubAck:
			log.Print("got PubAck")
			log.Print(message)

		case *mqtt.PubRec:
			log.Print("got PubRec, gonna send PubRel")
			log.Print(message)

		case *mqtt.PubComp:
			log.Print("got PubComp")
			log.Print(message)

		case *mqtt.Unsubscribe:
			log.Print("got Unsubscribe")
			log.Print(message)

		default:
			log.Print("got something else")
			log.Print(message)
		}
	}
}

func main() {
	listener, err := obtainListener(":2000")
	if err != nil {
		return
	}
	defer listener.Close()

	kafkaBrokers := []string{"localhost:9092"}
	kafkaConfig := sarama.NewConfig()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Print(err)
			return
		}

		go handleClient(conn, kafkaBrokers, kafkaConfig)
	}
}
