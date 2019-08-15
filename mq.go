package gomq

import (
	"fmt"
	"time"

	"github.com/streadway/amqp"
	"sync"
)

const BUFFER_SIZE = 500000

type Connection interface {
	GetChannel() (Channel, error)
	Close() error
}

type Channel interface {
	DeclareQueueByName(string) (Queue, error)
	Publish(string, string) error
}

type Queue interface {
	GetName() string
}

type RabbitConnection struct {
	NativeConnection *amqp.Connection
}

type RabbitChannel struct {
	NativeChannel *amqp.Channel
}

type RabbitQueue struct {
	NativeQueue *amqp.Queue
}

type NewConnection func(string) (Connection, error)

// return connection according to URI
func NewRabbitConnection(URI string) (Connection, error) {
	amqpNativeConn, err := amqp.Dial(URI)
	if err != nil {
		return nil, err
	}
	return RabbitConnection{amqpNativeConn}, nil
}

func (conn RabbitConnection) GetChannel() (Channel, error) {
	nativeChannel, err := conn.NativeConnection.Channel()
	if err != nil {
		return nil, err
	}

	return RabbitChannel{nativeChannel}, nil
}

func (conn RabbitConnection) Close() error {
	return conn.NativeConnection.Close()
}

func (channel RabbitChannel) DeclareQueueByName(queueName string) (Queue, error) {
	nativeQueue, err := channel.NativeChannel.QueueDeclare(
		queueName,
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return nil, err
	}

	return RabbitQueue{&nativeQueue}, nil
}

func (channel RabbitChannel) Publish(queueName string, message string) error {
	// if need reconnect, should handle here
	return channel.NativeChannel.Publish(
		"",
		queueName,
		false,
		false,
		amqp.Publishing{
			Body: []byte(message),
		},
	)
}

func (queue RabbitQueue) GetName() string {
	return queue.NativeQueue.Name
}

type MessageBody struct {
	QueueName string
	Message   string
}

type MQ struct {
	URI       string
	RetryTime time.Duration
	buffer    chan MessageBody
	channel   Channel
	mutex     *sync.RWMutex
}

func NewMQ(URI string, RetryTime time.Duration, newConnection NewConnection) *MQ {
	mq := &MQ{URI, RetryTime, make(chan MessageBody, BUFFER_SIZE), nil, &sync.RWMutex{}}
	mq.tryConnect(newConnection)
	go mq.listen(newConnection)
	return mq
}

func (mq *MQ) Connected() bool {
	return mq.channel != nil
}

func (mq *MQ) tryConnect(newConnection NewConnection) {
	if !mq.Connected() { // Not very thread-safe, but enough here
		defer mq.mutex.Unlock()
		mq.mutex.Lock()

		for {
			if mq.Connected() {
				break
			} else {
				connection, err := newConnection(mq.URI)
				if err != nil {
					fmt.Println("Get MQ connection failed", err)
					time.Sleep(mq.RetryTime)
					continue
				}
				channel, err := connection.GetChannel()

				if err != nil {
					connection.Close()
					fmt.Println("Get MQ channel failed", err)
					time.Sleep(mq.RetryTime)
					continue
				}

				mq.channel = channel
			}
		}
	}
}

func (mq *MQ) listen(newConnection NewConnection) {
	for mb := range mq.buffer {
		go func(mb MessageBody) {

			defer func() {
				if r := recover(); r != nil {
					fmt.Println("Recovered from panic", r)
					mq.tryConnect(newConnection)
				}
			}()

			queue, err := mq.channel.DeclareQueueByName(mb.QueueName)

			if err != nil {
				if err == amqp.ErrClosed {
					panic(err)
				}
				fmt.Printf("Encounter error when declare queue: %s\n", err)
				mq.buffer <- mb
				return
			}

			err = mq.channel.Publish(queue.GetName(), mb.Message)
			if err != nil {
				fmt.Printf("Encounter error when publish message: %s\n", err)
				mq.buffer <- mb
				return
			}
		}(mb)
	}
}

func (mq *MQ) Publish(queueName string, message string) {
	select {
	case mq.buffer <- MessageBody{queueName, message}:
	default:
		fmt.Println("Channel full. Discarding value")
	}
}
