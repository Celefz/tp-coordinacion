package messagehandler

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
	"github.com/google/uuid"
)

type MessageHandler struct {
	clientID string
}

func NewMessageHandler() MessageHandler {
	return MessageHandler{
		clientID: uuid.NewString(),
	}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	data := []fruititem.FruitItem{fruitRecord}
	return inner.SerializeMessage(messageHandler.clientID, data)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	data := []fruititem.FruitItem{}
	return inner.SerializeMessage(messageHandler.clientID, data)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	clientID, fruitRecords, _, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}
	if clientID != messageHandler.clientID {
		return nil, nil
	}
	return fruitRecords, nil
}
