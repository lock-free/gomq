package common

import (
	"fmt"
	"time"

	"github.com/streadway/amqp"
)

type MessageBody struct {
	QueueName string
	Message   string
}

type MQ struct {
	URI        string
	RetryTime  time.Duration
	buffer     chan MessageBody
	connection *amqp.Connection
	channel    *amqp.Channel
}

func NewMQ(URI string, RetryTime time.Duration) *MQ {
	mq := &MQ{URI, RetryTime, make(chan MessageBody, 500000), nil, nil}
	mq.tryConnect()
	go mq.listen()
	return mq
}

func (mq *MQ) tryConnect() {
	for {
		fmt.Println("Trying to connect")
		connection, err := amqp.Dial(mq.URI)

		if err != nil {
			fmt.Println("Get MQ connection failed", err)
			time.Sleep(mq.RetryTime)
			continue
		}

		mq.connection = connection
		channel, err := connection.Channel()

		if err != nil {
			connection.Close()
			fmt.Println("Get MQ channel failed", err)
			time.Sleep(mq.RetryTime)
			continue
		}

		mq.channel = channel
		break
	}
}

func (mq *MQ) listen() {
	for mb := range mq.buffer {
		func() {
			defer func() {
				if r := recover(); r != nil {
					fmt.Println("Recovered from panic", r)
					mq.tryConnect()
				}
			}()

			if mq.channel != nil {
				queue, err := mq.channel.QueueDeclare(
					mb.QueueName, // name
					false,        // durable
					false,        // delete when unused
					false,        // exclusive
					false,        // no-wait
					nil,          // arguments
				)

				if err != nil {
					mq.buffer <- mb
					panic(err)
				}

				err = mq.channel.Publish(
					"",         // exchange
					queue.Name, // routing key
					false,      // mandatory
					false,      // immediate
					amqp.Publishing{
						Body: []byte(mb.Message),
					})

				if err != nil {
					mq.buffer <- mb
					panic(err)
				}
			} else {
				fmt.Println("Channel has not been created")
			}
		}()
	}
}

func (mq *MQ) Publish(queueName string, message string) {
	select {
	case mq.buffer <- MessageBody{queueName, message}:
	default:
		fmt.Println("Channel full. Discarding value")
	}
}
