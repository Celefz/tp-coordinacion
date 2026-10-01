package join

import (
	"log/slog"
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
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
	join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleMessage(msg, ack, nack)
	})
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

	fruitTopRecords := join.buildFruitTop(clientID)
	message, err := inner.SerializeMessage(clientID, fruitTopRecords)
	if err != nil {
		slog.Debug("While serializing top message", "err", err)
		return err
	}
	if err := join.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending top message", "err", err)
		return err
	}

	eofMessage := []fruititem.FruitItem{}
	message, err = inner.SerializeMessage(clientID, eofMessage)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := join.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}

	delete(join.fruitItemMap, clientID)
	delete(join.endOfRecordCounts, clientID)

	return nil
}

func (join *Join) buildFruitTop(clientID string) []fruititem.FruitItem {
	fruitItems := join.fruitItemMap[clientID]
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(join.topSize, len(fruitItems))

	return fruitItems[:finalTopSize]
}
