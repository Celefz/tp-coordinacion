package middleware

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

const EXCHANGE_TYPE = "topic"

func CreateQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)

	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	queue, err := channel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		conn.Close()
		channel.Close()
		return nil, err
	}

	return NewQueueMiddleware(
		conn,
		channel,
		queue.Name,
	), nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings ConnSettings) (Middleware, error) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)

	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	err = channel.ExchangeDeclare(
		exchange,
		EXCHANGE_TYPE,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		conn.Close()
		channel.Close()
		return nil, err
	}

	queue, err := channel.QueueDeclare(
		"",
		false,
		true,
		true,
		false,
		nil,
	)
	if err != nil {
		conn.Close()
		channel.Close()
		return nil, err
	}

	for _, key := range keys {
		err = channel.QueueBind(
			queue.Name,
			key,
			exchange,
			false,
			nil,
		)
		if err != nil {
			conn.Close()
			channel.Close()
			return nil, err
		}
	}

	return NewExchangeMiddleware(
		conn,
		channel,
		exchange,
		queue.Name,
		keys,
	), nil
}
