package sum

import (
	"hash/crc32"
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
)

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

func (sum *Sum) completeClient(clientID string) error {
	sum.mutex.Lock()

	fruitItemMap := sum.fruitItemMaps[clientID]
	fruitRecords := make([]fruititem.FruitItem, 0, len(fruitItemMap))

	for _, fruitRecord := range fruitItemMap {
		fruitRecords = append(fruitRecords, fruitRecord)
	}

	sum.mutex.Unlock()

	for _, fruitRecord := range fruitRecords {
		message, err := inner.SerializeMessage(clientID, []fruititem.FruitItem{fruitRecord})
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}

		exchangeIndex := getExchangeIndex(fruitRecord.Fruit, uint32(len(sum.outputExchanges)))
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
