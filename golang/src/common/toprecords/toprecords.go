package toprecords

import (
	"fmt"
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

func GetTop(records []fruititem.FruitItem, size int) []fruititem.FruitItem {
	sort.SliceStable(records, func(i, j int) bool {
		return records[j].Less(records[i])
	})
	return records[:min(size, len(records))]
}

func Publish(clientID string, records []fruititem.FruitItem, output middleware.Middleware) error {
	message, err := inner.SerializeMessage(clientID, records)
	if err != nil {
		return fmt.Errorf("serializing top message: %w", err)
	}
	if err := output.Send(*message); err != nil {
		return fmt.Errorf("sending top message: %w", err)
	}

	eofMessage, err := inner.SerializeMessage(clientID, []fruititem.FruitItem{})
	if err != nil {
		return fmt.Errorf("serializing EOF message: %w", err)
	}
	if err := output.Send(*eofMessage); err != nil {
		return fmt.Errorf("sending EOF message: %w", err)
	}

	return nil
}
