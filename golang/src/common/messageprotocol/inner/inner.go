package inner

import (
	"encoding/json"
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

const (
	TYPE_CLIENT_MESSAGE = "client"
	TYPE_EOF_MESSAGE    = "eof"

	EOF_START     = "START"
	EOF_PROCESSED = "PROCESSED"
)

type clientMessage struct {
	Type     string        `json:"type"`
	ClientID string        `json:"client_id"`
	Records  []interface{} `json:"records"`
}

type EOFCoordMessage struct {
	Type     string `json:"type"`
	ClientID string `json:"client_id"`
	Kind     string `json:"kind"`
	Amount   int    `json:"amount"`
}

type messageType struct {
	Type string `json:"type"`
}

func serializeJson(message clientMessage) ([]byte, error) {
	return json.Marshal(message)
}

func deserializeJson(message []byte) (*clientMessage, error) {
	var data clientMessage
	if err := json.Unmarshal(message, &data); err != nil {
		return nil, err
	}
	if data.Type != TYPE_CLIENT_MESSAGE {
		return nil, errors.New("Message is not a client message")
	}
	return &data, nil
}

func DeserializeMessageType(message *middleware.Message) (string, error) {
	var data messageType
	if err := json.Unmarshal([]byte(message.Body), &data); err != nil {
		return "", err
	}
	if data.Type != TYPE_CLIENT_MESSAGE && data.Type != TYPE_EOF_MESSAGE {
		return "", errors.New("Unknown inner message type")
	}
	return data.Type, nil
}

func SerializeMessage(clientID string, fruitRecords []fruititem.FruitItem) (*middleware.Message, error) {
	data := []interface{}{}
	for _, fruitRecord := range fruitRecords {
		datum := []interface{}{
			fruitRecord.Fruit,
			fruitRecord.Amount,
		}
		data = append(data, datum)
	}

	clientRecords := clientMessage{
		Type:     TYPE_CLIENT_MESSAGE,
		ClientID: clientID,
		Records:  data,
	}

	body, err := serializeJson(clientRecords)
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func DeserializeMessage(message *middleware.Message) (string, []fruititem.FruitItem, bool, error) {
	data, err := deserializeJson([]byte((*message).Body))
	if err != nil {
		return "", nil, false, err
	}

	fruitRecords := []fruititem.FruitItem{}

	for _, datum := range data.Records {
		fruitPair, ok := datum.([]interface{})
		if !ok {
			return "", nil, false, errors.New("Datum is not an array")
		}

		fruit, ok := fruitPair[0].(string)
		if !ok {
			return "", nil, false, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitAmount, ok := fruitPair[1].(float64)
		if !ok {
			return "", nil, false, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitRecord := fruititem.FruitItem{Fruit: fruit, Amount: uint32(fruitAmount)}
		fruitRecords = append(fruitRecords, fruitRecord)
	}

	return data.ClientID, fruitRecords, len(fruitRecords) == 0, nil
}

func SerializeEOFMessage(clientID string, kind string, amount int) (*middleware.Message, error) {
	eofMessage := EOFCoordMessage{
		Type:     TYPE_EOF_MESSAGE,
		ClientID: clientID,
		Kind:     kind,
		Amount:   amount,
	}
	body, err := json.Marshal(eofMessage)
	if err != nil {
		return nil, err
	}

	return &middleware.Message{Body: string(body)}, nil
}

func DeserializeEOFMessage(message *middleware.Message) (EOFCoordMessage, error) {
	var eofMessage EOFCoordMessage

	if err := json.Unmarshal([]byte(message.Body), &eofMessage); err != nil {
		return EOFCoordMessage{}, err
	}
	if eofMessage.Type != TYPE_EOF_MESSAGE {
		return EOFCoordMessage{}, errors.New("Message is not an EOF message")
	}

	return eofMessage, nil
}
