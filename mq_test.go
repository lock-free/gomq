package gomq

import (
	"testing"
	"time"
	"errors"
	"sync"
)

type MockConnection struct {}
type MockChannel struct {}
type MockQueue struct {
	Name string
}

var mutex sync.RWMutex

var errorMap = map[string]error {
	"NewConnection": nil,
	"GetChannel": nil,
	"Close": nil,
	"DeclareQueueByName": nil,
	"Publish": nil,
}

func getErrorByName(name string) error {
	mutex.RLock()
	defer mutex.RUnlock()
	return errorMap[name] 
}

func resetErrorMap() {
	mutex.Lock()
	defer mutex.Unlock()
	errorMap = map[string]error {
		"NewConnection": nil,
		"GetChannel": nil,
		"Close": nil,
		"DeclareQueueByName": nil,
		"Publish": nil,
	} 
}

func updateErrorMap(key string) {
	mutex.Lock()
	defer mutex.Unlock()
	errorMap[key] = errors.New(key + " failed")
} 

func NewMockConnection(URI string) (Connection, error) {
	return MockConnection{}, getErrorByName("NewMockConnection")
}

func (conn MockConnection) GetChannel() (Channel, error) {
	return MockChannel{}, getErrorByName("GetChannel") 
}

func (conn MockConnection) Close() error {
	return getErrorByName("Close")
}

func (queue MockQueue) GetName() string {
	return queue.Name
}

func (channel MockChannel) DeclareQueueByName(queueName string) (Queue, error) {
	return MockQueue{queueName}, getErrorByName("DeclareQueueByName")
}

func (channel MockChannel) Publish(queueName string, message string) error {
	return getErrorByName("Publish")
}

func TestPublish(t *testing.T) {
	mq := NewMQ("test-url", 2*time.Nanosecond, NewMockConnection)
	mq.Publish("test-queue", "test message")
}

func TestNewConnectionFailed(t *testing.T) {
	updateErrorMap("NewConnection")
	defer resetErrorMap()
	mq := NewMQ("test-url", 2*time.Nanosecond, NewMockConnection)
	mq.Publish("test-queue", "test message")
}

func TestGetChannelFailed(t *testing.T) {
	updateErrorMap("GetChannel")
	defer resetErrorMap()
	mq := NewMQ("test-url", 2*time.Nanosecond, NewMockConnection)
	mq.Publish("test-queue", "test message")
}

func TestCrazyPublish(t *testing.T) {
	mq := NewMQ("test-url", 2*time.Nanosecond, NewMockConnection)
	for i := 0; i < 100; i ++ {
		mq.Publish("test-queue", "test message")
	}
}