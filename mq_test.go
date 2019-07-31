package gomq

import (
	"testing"
	"time"
	"errors"
	"math/rand"
)

type MockConnection struct {}
type MockChannel struct {}
type MockQueue struct {
	Name string
}

func getRandError(errs []error) error {
	rand.Seed(time.Now().Unix()) // initialize global pseudo random generator
	return errs[rand.Intn(len(errs))] 
}

func NewMockConnection(URI string) (Connection, error) {
	errs := []error{
		errors.New("New connection failed"),
		nil,
	}
	return MockConnection{}, getRandError(errs)
}

func (conn MockConnection) GetChannel() (Channel, error) {
	errs := []error{
		errors.New("Get channel failed"),
		nil,
	}
	return MockChannel{}, getRandError(errs) 
}

func (conn MockConnection) Close() error {
	errs := []error{
		errors.New("Close failed"),
		nil,
	}
	return getRandError(errs)
}

func (queue MockQueue) GetName() string {
	return queue.Name
}

func (channel MockChannel) DeclareQueueByName(queueName string) (Queue, error) {
	errs := []error{
		errors.New("Declare queue failed"),
		nil,
	}
	return MockQueue{queueName}, getRandError(errs)
}

func (channel MockChannel) Publish(queueName string, message string) error {

	errs := []error{
		errors.New("Publish failed"),
		nil,
	}
	return getRandError(errs)
}

func TestPublish(t *testing.T) {
	mq := NewMQ("test-url", 2*time.Second, NewMockConnection)
	mq.Publish("test-queue", "test message")
}

func TestCrazyPublish(t *testing.T) {
	mq := NewMQ("test-url", 2*time.Second, NewMockConnection)
	for i := 0; i < BUFFER_SIZE + 1; i ++ {
		mq.Publish("test-queue", "test message")
	}
}
