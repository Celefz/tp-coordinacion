package join

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/toprecords"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Join struct {
	inputQueue        middleware.Middleware
	outputQueue       middleware.Middleware
	fruitItemMap      map[string][]fruititem.FruitItem
	endOfRecordCounts map[string]int
	aggregationAmount int
	topSize           int
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Join{
		inputQueue:        inputQueue,
		outputQueue:       outputQueue,
		fruitItemMap:      map[string][]fruititem.FruitItem{},
		endOfRecordCounts: map[string]int{},
		aggregationAmount: config.AggregationAmount,
		topSize:           config.TopSize,
	}, nil
}

func (join *Join) Run() {
	go join.handleSignals()

	join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleMessage(msg, ack, nack)
	})
}

func (join *Join) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)
	<-signals
	slog.Info("SIGTERM signal received")

	if err := join.outputQueue.Close(); err != nil {
		slog.Error("While closing output queue", "err", err)
	}
	if err := join.inputQueue.Close(); err != nil {
		slog.Error("While closing input queue", "err", err)
	}
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientID, fruitRecords, isEOF, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEOF {
		if err := join.handleEndOfRecordsMessage(clientID); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	join.handleDataMessage(clientID, fruitRecords)
}

func (join *Join) handleDataMessage(clientID string, fruitRecords []fruititem.FruitItem) {
	join.fruitItemMap[clientID] = append(
		join.fruitItemMap[clientID],
		fruitRecords...,
	)
}

func (join *Join) handleEndOfRecordsMessage(clientID string) error {
	slog.Info("Received End Of Records message")

	join.endOfRecordCounts[clientID]++
	if join.endOfRecordCounts[clientID] < join.aggregationAmount {
		return nil
	}

	if err := toprecords.Publish(clientID, join.buildFruitTop(clientID), join.outputQueue); err != nil {
		slog.Debug("While publishing top and EOF messages", "err", err)
		return err
	}

	delete(join.fruitItemMap, clientID)
	delete(join.endOfRecordCounts, clientID)

	return nil
}

func (join *Join) buildFruitTop(clientID string) []fruititem.FruitItem {
	fruitItems := join.fruitItemMap[clientID]
	return toprecords.GetTop(fruitItems, join.topSize)
}
