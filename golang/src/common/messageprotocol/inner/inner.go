package inner

import (
	"encoding/json"
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type clientMessage struct {
	ClientID string        `json:"client_id"`
	Records  []interface{} `json:"records"`
}

func serializeJson(message clientMessage) ([]byte, error) {
	return json.Marshal(message)
}

func deserializeJson(message []byte) (*clientMessage, error) {
	var data clientMessage
	if err := json.Unmarshal(message, &data); err != nil {
		return nil, err
	}
	return &data, nil
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
