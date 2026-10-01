package middleware

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	common    *CommonMiddlewareInfo
	queueName string
}

func NewQueueMiddleware(conn *amqp.Connection, channel *amqp.Channel, queueName string) *QueueMiddleware {
	return &QueueMiddleware{
		common:    NewCommonMiddlewareInfo(conn, channel, queueName),
		queueName: queueName,
	}
}

func (q *QueueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	return q.common.StartConsuming(q.queueName, callbackFunc)
}

func (q *QueueMiddleware) StopConsuming() error {
	return q.common.StopConsuming()
}

func (q *QueueMiddleware) Send(msg Message) error {
	if err := q.common.ValidateConnection(); err != nil {
		return err
	}

	err := q.common.channel.Publish(
		"",
		q.queueName,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		},
	)

	if err != nil {
		return ErrMessageMiddlewareMessage
	}
	return nil
}

func (q *QueueMiddleware) Close() error {
	return q.common.Close()
}
