package sum

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/myhashing"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/recordkey"
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type Sum struct {
	inputQueue        middleware.Middleware
	outputExchange    middleware.Middleware
	broadcastExchange middleware.Middleware
	fruitItemMap      map[recordkey.FruitKey]fruititem.FruitItem
	mutex             sync.Mutex
	aggregationKeys   map[uint32]string
	aggAmount         uint32
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}
	aggregationKeys := map[uint32]string{}

	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
		aggregationKeys[uint32(i)] = outputExchangeRouteKeys[i]
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	broadcastExchange, err := middleware.CreateExchangeMiddleware("sum broadcast", []string{"EoF"}, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		return nil, err
	}

	return &Sum{
		inputQueue:        inputQueue,
		outputExchange:    outputExchange,
		broadcastExchange: broadcastExchange,
		fruitItemMap:      map[recordkey.FruitKey]fruititem.FruitItem{},
		aggregationKeys:   aggregationKeys,
		aggAmount:         uint32(config.AggregationAmount),
	}, nil
}

func (sum *Sum) Run() {
	go sum.broadcastExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleBroadcastFromOtherSum(msg, ack, nack)
	})
	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	fruitRecords, isEof, clientID, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if clientID == 0 {
		slog.Error("Records have no client associated")
		return
	}

	if isEof {
		if err := sum.handleEndOfRecordMessage(clientID); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	if err := sum.handleDataMessage(fruitRecords, clientID); err != nil {
		slog.Error("While handling data message", "err", err)
	}
}

func (sum *Sum) handleBroadcastFromOtherSum(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	_, isEof, clientID, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if clientID == 0 {
		slog.Error("Records have no client associated")
		return
	}

	if isEof {
		if err := sum.handleEndOfRecordBroadcast(clientID); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}
}

func (sum *Sum) handleEndOfRecordBroadcast(clientID uint32) error {
	slog.Info("Received End Of Records message")
	sum.mutex.Lock()
	defer sum.mutex.Unlock()
	for key := range sum.fruitItemMap {
		if key.ClientID != clientID {
			continue
		}
		fruitRecord := []fruititem.FruitItem{sum.fruitItemMap[key]}
		message, err := inner.SerializeMessage(fruitRecord, clientID)
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}
		hash := myhashing.HashString(key, sum.aggAmount)
		key := sum.aggregationKeys[hash]
		if err := sum.outputExchange.SendWithKeys(*message, []string{key}); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}

	eofMessage := []fruititem.FruitItem{}
	message, err := inner.SerializeMessage(eofMessage, clientID)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := sum.outputExchange.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	return nil
}

func (sum *Sum) handleEndOfRecordMessage(clientID uint32) error {
	slog.Info("Received Endof Record message")
	eofMessage := []fruititem.FruitItem{}
	message, err := inner.SerializeMessage(eofMessage, clientID)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := sum.broadcastExchange.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	return nil
}

func (sum *Sum) handleDataMessage(fruitRecords []fruititem.FruitItem, clientID uint32) error {
	sum.mutex.Lock()
	defer sum.mutex.Unlock()
	for _, fruitRecord := range fruitRecords {
		recordKey := recordkey.FruitKey{ClientID: clientID, FruitName: fruitRecord.Fruit}
		_, ok := sum.fruitItemMap[recordKey]
		if ok {
			sum.fruitItemMap[recordKey] = sum.fruitItemMap[recordKey].Sum(fruitRecord)
		} else {
			sum.fruitItemMap[recordKey] = fruitRecord
		}
	}
	return nil
}

func (sum *Sum) Close() {
	sum.inputQueue.Close()
	sum.outputExchange.Close()
	sum.broadcastExchange.Close()
}
