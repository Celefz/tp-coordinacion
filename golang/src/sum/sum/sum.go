package sum

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
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
	outputExchange    middleware.Middleware
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

	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	controlExchange, err := middleware.CreateExchangeMiddleware(config.SumPrefix+SUM_CONTROL_SUFFIX, []string{EOF_CONTROL_KEY}, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		return nil, err
	}

	return &Sum{
		inputQueue:        inputQueue,
		outputExchange:    outputExchange,
		controlExchange:   controlExchange,
		fruitItemMaps:     map[string]map[string]fruititem.FruitItem{},
		localProcessed:    map[string]int{},
		expectedProcessed: map[string]int{},
		totalProcessed:    map[string]int{},
	}, nil
}

func (sum *Sum) Run() {
	go sum.controlExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleControlMessage(msg, ack, nack)
	})

	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	eofMessage, err := inner.DeserializeEOFMessage(&msg)

	if err == nil && eofMessage.Kind == inner.EOF_START {
		if err := sum.controlExchange.Send(msg); err != nil {
			slog.Error("While sending EOF start message", "err", err)
		}

		return
	}

	clientID, fruitRecords, _, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if err := sum.handleDataMessage(clientID, fruitRecords); err != nil {
		slog.Error("While handling data message", "err", err)
	}
}

func (sum *Sum) handleControlMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	eofMessage, err := inner.DeserializeEOFMessage(&msg)
	if err != nil {
		slog.Error("While deserializing EOF message", "err", err)
		return
	}

	switch eofMessage.Kind {

	case inner.EOF_START:
		if err := sum.handleEOFStart(eofMessage); err != nil {
			slog.Error("While handling EOF start", "err", err)
		}

	case inner.EOF_PROCESSED:
		sum.handleProcessedCount(eofMessage)

	default:
		slog.Error("Unknown EOF message type", "type", eofMessage.Kind)
	}
}

func (sum *Sum) handleEOFStart(eofMessage inner.EOFMessage) error {
	sum.mutex.Lock()

	if _, exists := sum.expectedProcessed[eofMessage.ClientID]; exists {
		sum.mutex.Unlock()
		return nil
	}

	sum.expectedProcessed[eofMessage.ClientID] = eofMessage.Amount
	localCount := sum.localProcessed[eofMessage.ClientID]

	sum.mutex.Unlock()

	if localCount > 0 {
		if err := sum.sendProcessedCount(eofMessage.ClientID, localCount); err != nil {
			return err
		}
	}

	return nil
}

func (sum *Sum) sendProcessedCount(clientID string, amount int) error {
	message, err := inner.SerializeEOFMessage(clientID, inner.EOF_PROCESSED, amount)

	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}

	return sum.controlExchange.Send(*message)
}

func (sum *Sum) handleProcessedCount(eofMessage inner.EOFMessage) {
	sum.mutex.Lock()

	sum.totalProcessed[eofMessage.ClientID] += eofMessage.Amount
	total := sum.totalProcessed[eofMessage.ClientID]
	expected, exists := sum.expectedProcessed[eofMessage.ClientID]

	sum.mutex.Unlock()

	if exists && total == expected {
		if err := sum.completeClient(eofMessage.ClientID); err != nil {
			slog.Error("While completing client", "err", err)
		}
	}
}

func (sum *Sum) handleDataMessage(clientID string, fruitRecords []fruititem.FruitItem) error {
	sum.mutex.Lock()

	fruitItemMap, ok := sum.fruitItemMaps[clientID]
	if !ok {
		fruitItemMap = map[string]fruititem.FruitItem{}
		sum.fruitItemMaps[clientID] = fruitItemMap
	}

	for _, fruitRecord := range fruitRecords {
		_, ok := sum.fruitItemMaps[clientID][fruitRecord.Fruit]
		if ok {
			sum.fruitItemMaps[clientID][fruitRecord.Fruit] = sum.fruitItemMaps[clientID][fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			sum.fruitItemMaps[clientID][fruitRecord.Fruit] = fruitRecord
		}
	}
	sum.localProcessed[clientID] += len(fruitRecords)

	_, eofStarted := sum.expectedProcessed[clientID]

	sum.mutex.Unlock()

	if eofStarted && len(fruitRecords) > 0 {
		if err := sum.sendProcessedCount(clientID, len(fruitRecords)); err != nil {
			return err
		}
	}

	return nil
}

func (sum *Sum) completeClient(clientID string) error {
	sum.mutex.Lock()

	fruitItemMap := sum.fruitItemMaps[clientID]
	fruitRecords := make([]fruititem.FruitItem, 0, len(fruitItemMap))

	for _, fruitRecord := range fruitItemMap {
		fruitRecords = append(
			fruitRecords,
			fruitRecord,
		)
	}

	sum.mutex.Unlock()

	for _, fruitRecord := range fruitRecords {

		message, err := inner.SerializeMessage(clientID, []fruititem.FruitItem{fruitRecord})

		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}

		if err := sum.outputExchange.Send(*message); err != nil {
			return err
		}
	}
	eofMessage, err := inner.SerializeMessage(clientID, []fruititem.FruitItem{})

	if err != nil {
		return err
	}

	return sum.outputExchange.Send(*eofMessage)
}
