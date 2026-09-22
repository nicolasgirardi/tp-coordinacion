package inner

import (
	"encoding/json"
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

func serializeJson(message []interface{}) ([]byte, error) {
	return json.Marshal(message)
}

func deserializeJson(message []byte) ([]interface{}, error) {
	var data []interface{}
	if err := json.Unmarshal(message, &data); err != nil {
		return nil, err
	}
	return data, nil
}

func SerializeMessage(fruitRecords []fruititem.FruitItem, clientID uint32) (*middleware.Message, error) {
	client := []interface{}{"clientID", clientID}
	data := []interface{}{client}
	for _, fruitRecord := range fruitRecords {
		datum := []interface{}{
			fruitRecord.Fruit,
			fruitRecord.Amount,
		}
		data = append(data, datum)
	}

	body, err := serializeJson(data)
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func DeserializeMessage(message *middleware.Message) ([]fruititem.FruitItem, bool, uint32, error) {
	data, err := deserializeJson([]byte((*message).Body))
	if err != nil {
		return nil, false, 0, err
	}
	clientID := uint32(0)

	fruitRecords := []fruititem.FruitItem{}
	for _, datum := range data {
		fruitPair, ok := datum.([]interface{})
		if !ok {
			return nil, false, 0, errors.New("Datum is not an array")
		}

		fruit, ok := fruitPair[0].(string)
		if !ok {
			return nil, false, 0, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitAmount, ok := fruitPair[1].(float64)
		if !ok {
			return nil, false, 0, errors.New("Datum is not a (fruit, amount) pair")
		}

		if fruit == "clientID" {
			clientID = uint32(fruitAmount)
		} else {
			fruitRecord := fruititem.FruitItem{Fruit: fruit, Amount: uint32(fruitAmount)}
			fruitRecords = append(fruitRecords, fruitRecord)
		}
	}

	return fruitRecords, len(fruitRecords) == 0, clientID, nil
}
