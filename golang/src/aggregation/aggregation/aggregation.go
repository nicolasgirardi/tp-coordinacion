package aggregation

import (
	"fmt"
	"log/slog"
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/recordkey"
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
	fruitItemMap      map[recordkey.FruitKey]fruititem.FruitItem
	topSize           int
	sumAmount         int
	endOfRegistersMap map[uint32]int
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
		fruitItemMap:      map[recordkey.FruitKey]fruititem.FruitItem{},
		topSize:           config.TopSize,
		sumAmount:         config.SumAmount,
		endOfRegistersMap: map[uint32]int{},
	}, nil
}

func (aggregation *Aggregation) Run() {
	aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	fruitRecords, isEof, clientID, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if clientID == 0 {
		slog.Error("Client ID is invalid")
		return
	}

	if isEof {
		if err := aggregation.handleEndOfRecordsMessage(clientID); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	aggregation.handleDataMessage(fruitRecords, clientID)
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(clientID uint32) error {
	slog.Info("Received End Of Records message")
	_, ok := aggregation.endOfRegistersMap[clientID]
	if ok {
		aggregation.endOfRegistersMap[clientID] = aggregation.endOfRegistersMap[clientID] + 1
	} else {
		aggregation.endOfRegistersMap[clientID] = 1
	}

	if aggregation.endOfRegistersMap[clientID] == aggregation.sumAmount {
		fruitTopRecords := aggregation.buildFruitTop(clientID)
		message, err := inner.SerializeMessage(fruitTopRecords, clientID)
		if err != nil {
			slog.Debug("While serializing top message", "err", err)
			return err
		}
		if err := aggregation.outputQueue.Send(*message); err != nil {
			slog.Debug("While sending top message", "err", err)
			return err
		}

		eofMessage := []fruititem.FruitItem{}
		message, err = inner.SerializeMessage(eofMessage, clientID)
		if err != nil {
			slog.Debug("While serializing EOF message", "err", err)
			return err
		}
		if err := aggregation.outputQueue.Send(*message); err != nil {
			slog.Debug("While sending EOF message", "err", err)
			return err
		}
	}
	return nil
}

func (aggregation *Aggregation) handleDataMessage(fruitRecords []fruititem.FruitItem, clientID uint32) {
	for _, fruitRecord := range fruitRecords {
		recordKey := recordkey.FruitKey{ClientID: clientID, FruitName: fruitRecord.Fruit}
		if _, ok := aggregation.fruitItemMap[recordKey]; ok {
			aggregation.fruitItemMap[recordKey] = aggregation.fruitItemMap[recordKey].Sum(fruitRecord)
		} else {
			aggregation.fruitItemMap[recordKey] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) buildFruitTop(clientID uint32) []fruititem.FruitItem {
	fruitItems := make([]fruititem.FruitItem, 0, len(aggregation.fruitItemMap))
	for key, item := range aggregation.fruitItemMap {
		if key.ClientID == clientID {
			fruitItems = append(fruitItems, item)
		}
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(aggregation.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}

func (Aggregation *Aggregation) Close(){
	Aggregation.inputExchange.Close()
	Aggregation.outputQueue.Close()
}