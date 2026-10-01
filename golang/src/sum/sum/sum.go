package sum

import (
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

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

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	messageType, err := inner.DeserializeMessageType(&msg)
	if err != nil {
		slog.Error("While deserializing message type", "err", err)
		return
	}

	switch messageType {
	case inner.TYPE_EOF_MESSAGE:
		eofCoordMessage, err := inner.DeserializeEOFMessage(&msg)
		if err != nil {
			slog.Error("While deserializing EOF message", "err", err)
			return
		}
		if eofCoordMessage.Kind != inner.EOF_START {
			slog.Error("Unexpected EOF message on input queue", "type", eofCoordMessage.Kind)
			return
		}
		if err := sum.controlExchange.Send(msg); err != nil {
			slog.Error("While sending EOF start message", "err", err)
		}

	case inner.TYPE_CLIENT_MESSAGE:
		clientID, fruitRecords, _, err := inner.DeserializeMessage(&msg)
		if err != nil {
			slog.Error("While deserializing message", "err", err)
			return
		}

		if err := sum.handleDataMessage(clientID, fruitRecords); err != nil {
			slog.Error("While handling data message", "err", err)
		}
	}
}

func (sum *Sum) handleControlMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	eofCoordMessage, err := inner.DeserializeEOFMessage(&msg)
	if err != nil {
		slog.Error("While deserializing EOF message", "err", err)
		return
	}

	switch eofCoordMessage.Kind {

	case inner.EOF_START:
		if err := sum.handleEOFStart(eofCoordMessage); err != nil {
			slog.Error("While handling EOF start", "err", err)
		}

	case inner.EOF_PROCESSED:
		sum.handleProcessedCount(eofCoordMessage)

	default:
		slog.Error("Unknown EOF message type", "type", eofCoordMessage.Kind)
	}
}

func (sum *Sum) handleEOFStart(eofCoordMessage inner.EOFCoordMessage) error {
	sum.mutex.Lock()

	if _, exists := sum.expectedProcessed[eofCoordMessage.ClientID]; exists {
		sum.mutex.Unlock()
		return nil
	}

	sum.expectedProcessed[eofCoordMessage.ClientID] = eofCoordMessage.Amount
	localCount := sum.localProcessed[eofCoordMessage.ClientID]

	sum.mutex.Unlock()

	if localCount > 0 {
		if err := sum.sendProcessedCount(eofCoordMessage.ClientID, localCount); err != nil {
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

func (sum *Sum) handleProcessedCount(eofCoordMessage inner.EOFCoordMessage) {
	sum.mutex.Lock()

	sum.totalProcessed[eofCoordMessage.ClientID] += eofCoordMessage.Amount
	total := sum.totalProcessed[eofCoordMessage.ClientID]
	expected, exists := sum.expectedProcessed[eofCoordMessage.ClientID]

	sum.mutex.Unlock()

	if exists && total == expected {
		if err := sum.completeClient(eofCoordMessage.ClientID); err != nil {
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

		exchangeIndex := getExchangeIndex(
			fruitRecord.Fruit,
			uint32(len(sum.outputExchanges)),
		)

		if err := sum.outputExchanges[exchangeIndex].Send(*message); err != nil {
			return err
		}
	}

	eofCoordMessage, err := inner.SerializeMessage(clientID, []fruititem.FruitItem{})
	if err != nil {
		return err
	}

	for _, exchange := range sum.outputExchanges {
		if err := exchange.Send(*eofCoordMessage); err != nil {
			return err
		}
	}
	return nil
}

func getExchangeIndex(fruit string, numExchanges uint32) int {
	hash := crc32.ChecksumIEEE([]byte(fruit))
	return int(hash % numExchanges)
}
