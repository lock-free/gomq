# gomq

RabbitMQ kit for Golang

## Features
1. Has a buffer channel for holding the message temporarily
2. When publishing message failed, will push the failed message into the buffer again and reconnect RabbitMQ server
