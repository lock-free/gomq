package gomq

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestPublish(t *testing.T) {
	if url := os.Getenv("MQ_URL"); url != "" {
		mq := NewMQ(url, 2*time.Second)
		mq.Publish("test", "test message")
	} else {
		fmt.Println("MQ_URL not set")
		return
	}
}
