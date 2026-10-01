package aggregation

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/toprecords"
)

type AggregationConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Aggregation struct {
	outputQueue       middleware.Middleware
	inputExchange     middleware.Middleware
	fruitItemMaps     map[string]map[string]fruititem.FruitItem
	endOfRecordCounts map[string]int
	sumAmount         int
	topSize           int
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputExchangeRoutingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		outputQueue.Close()
		return nil, err
	}

	return &Aggregation{
		outputQueue:       outputQueue,
		inputExchange:     inputExchange,
		fruitItemMaps:     map[string]map[string]fruititem.FruitItem{},
		endOfRecordCounts: map[string]int{},
		sumAmount:         config.SumAmount,
		topSize:           config.TopSize,
	}, nil
}

func (aggregation *Aggregation) Run() {
	go aggregation.handleSignals()

	aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
}

func (aggregation *Aggregation) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)
	<-signals
	slog.Info("SIGTERM signal received")

	if err := aggregation.outputQueue.Close(); err != nil {
		slog.Error("While closing output queue", "err", err)
	}
	if err := aggregation.inputExchange.Close(); err != nil {
		slog.Error("While closing input exchange", "err", err)
	}
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientID, fruitRecords, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		if err := aggregation.handleEndOfRecordsMessage(clientID); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	aggregation.handleDataMessage(clientID, fruitRecords)
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(clientID string) error {
	slog.Info("Received End Of Records message")
	if aggregation.endOfRecordCounts[clientID] >= aggregation.sumAmount {
		return nil
	}
	aggregation.endOfRecordCounts[clientID]++
	if aggregation.endOfRecordCounts[clientID] < aggregation.sumAmount {
		return nil
	}

	if err := toprecords.Publish(clientID, aggregation.buildFruitTop(clientID), aggregation.outputQueue); err != nil {
		slog.Debug("While publishing top and EOF messages", "err", err)
		return err
	}

	delete(aggregation.fruitItemMaps, clientID)
	delete(aggregation.endOfRecordCounts, clientID)

	return nil
}

func (aggregation *Aggregation) handleDataMessage(clientID string, fruitRecords []fruititem.FruitItem) {
	fruitItemMap, ok := aggregation.fruitItemMaps[clientID]
	if !ok {
		fruitItemMap = map[string]fruititem.FruitItem{}
		aggregation.fruitItemMaps[clientID] = fruitItemMap
	}

	for _, fruitRecord := range fruitRecords {
		if _, ok := aggregation.fruitItemMaps[clientID][fruitRecord.Fruit]; ok {
			aggregation.fruitItemMaps[clientID][fruitRecord.Fruit] = aggregation.fruitItemMaps[clientID][fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			aggregation.fruitItemMaps[clientID][fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) buildFruitTop(clientID string) []fruititem.FruitItem {
	clientFruitItems := aggregation.fruitItemMaps[clientID]

	fruitItems := make([]fruititem.FruitItem, 0, len(clientFruitItems))
	for _, item := range clientFruitItems {
		fruitItems = append(fruitItems, item)
	}
	return toprecords.GetTop(fruitItems, aggregation.topSize)
}
