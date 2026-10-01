package middleware

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

const CONSUMER_TAG_SUFFIX = "-consumer"

type CommonMiddlewareInfo struct {
	connection  *amqp.Connection
	channel     *amqp.Channel
	consumerTag string
}

func NewCommonMiddlewareInfo(conn *amqp.Connection, channel *amqp.Channel, consumerTagPreffix string) *CommonMiddlewareInfo {
	consumerTag := consumerTagPreffix + CONSUMER_TAG_SUFFIX

	return &CommonMiddlewareInfo{
		connection:  conn,
		channel:     channel,
		consumerTag: consumerTag,
	}
}

func (c *CommonMiddlewareInfo) ValidateConnection() error {
	if c.connection.IsClosed() || c.channel.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (c *CommonMiddlewareInfo) StartConsuming(queueName string, callbackFunc func(msg Message, ack func(), nack func())) error {
	if err := c.ValidateConnection(); err != nil {
		return err
	}

	messages, err := c.channel.Consume(
		queueName,
		c.consumerTag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return ErrMessageMiddlewareMessage
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for message := range messages {
			callbackFunc(
				Message{Body: string(message.Body)},
				func() { message.Ack(false) },
				func() { message.Nack(false, true) },
			)
		}
	}()
	<-done

	return nil
}

func (c *CommonMiddlewareInfo) StopConsuming() error {
	if err := c.ValidateConnection(); err != nil {
		return err
	}

	if err := c.channel.Cancel(c.consumerTag, false); err != nil {
		return ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (c *CommonMiddlewareInfo) Close() error {
	channelErr := c.channel.Close()
	connectionErr := c.connection.Close()

	if channelErr != nil || connectionErr != nil {
		return ErrMessageMiddlewareClose
	}
	return nil
}
