package middleware

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeMiddleware struct {
	common    *CommonMiddlewareInfo
	exchange  string
	queueName string
	keys      []string
}

func NewExchangeMiddleware(
	conn *amqp.Connection,
	channel *amqp.Channel,
	exchangeName string,
	queueName string,
	keys []string,
) *ExchangeMiddleware {
	return &ExchangeMiddleware{
		common:    NewCommonMiddlewareInfo(conn, channel, queueName),
		exchange:  exchangeName,
		queueName: queueName,
		keys:      keys,
	}
}

func (e *ExchangeMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	return e.common.StartConsuming(e.queueName, callbackFunc)
}

func (e *ExchangeMiddleware) StopConsuming() error {
	return e.common.StopConsuming()
}

func (e *ExchangeMiddleware) Send(msg Message) error {
	if err := e.common.ValidateConnection(); err != nil {
		return err
	}

	for _, key := range e.keys {
		err := e.common.channel.Publish(
			e.exchange,
			key,
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
	}
	return nil
}

func (e *ExchangeMiddleware) Close() error {
	return e.common.Close()
}
