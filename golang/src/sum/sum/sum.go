package sum

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

const (
	EOF_CONTROL_KEY    = "eof"
	SUM_CONTROL_SUFFIX = "_control"
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type Sum struct {
	inputQueue        middleware.Middleware
	outputExchanges   []middleware.Middleware
	controlExchange   middleware.Middleware
	fruitItemMaps     map[string]map[string]fruititem.FruitItem
	mutex             sync.Mutex
	localProcessed    map[string]int
	expectedProcessed map[string]int
	totalProcessed    map[string]int
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchanges := make([]middleware.Middleware, 0, config.AggregationAmount)

	for i := 0; i < config.AggregationAmount; i++ {
		routingKey := fmt.Sprintf("%s_%d", config.AggregationPrefix, i)

		outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, []string{routingKey}, connSettings)
		if err != nil {
			inputQueue.Close()
			for _, exchange := range outputExchanges {
				exchange.Close()
			}
			return nil, err
		}

		outputExchanges = append(outputExchanges, outputExchange)
	}

	controlExchange, err := middleware.CreateExchangeMiddleware(config.SumPrefix+SUM_CONTROL_SUFFIX, []string{EOF_CONTROL_KEY}, connSettings)
	if err != nil {
		inputQueue.Close()
		for _, exchange := range outputExchanges {
			exchange.Close()
		}
		return nil, err
	}

	return &Sum{
		inputQueue:        inputQueue,
		outputExchanges:   outputExchanges,
		controlExchange:   controlExchange,
		fruitItemMaps:     map[string]map[string]fruititem.FruitItem{},
		localProcessed:    map[string]int{},
		expectedProcessed: map[string]int{},
		totalProcessed:    map[string]int{},
	}, nil
}

func (sum *Sum) Run() {
	go sum.handleSignals()

	go sum.controlExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleControlMessage(msg, ack, nack)
	})

	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
}

func (sum *Sum) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)
	<-signals
	slog.Info("SIGTERM signal received")

	if err := sum.controlExchange.Close(); err != nil {
		slog.Error("While closing control exchange", "err", err)
	}
	for _, exchange := range sum.outputExchanges {
		if err := exchange.Close(); err != nil {
			slog.Error("While closing output exchange", "err", err)
		}
	}
	if err := sum.inputQueue.Close(); err != nil {
		slog.Error("While closing input queue", "err", err)
	}
}
