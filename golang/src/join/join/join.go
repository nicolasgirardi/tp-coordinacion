package join

import (
	"log/slog"
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/recordkey"
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
	fruitItemMap      map[recordkey.FruitKey]fruititem.FruitItem
	topSize           int
	aggAmount         int
	endOfRegistersMap map[uint32]int
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
		fruitItemMap:      map[recordkey.FruitKey]fruititem.FruitItem{},
		topSize:           config.TopSize,
		aggAmount:         config.AggregationAmount,
		endOfRegistersMap: map[uint32]int{},
	}, nil
}

func (join *Join) Run() {
	join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleMessage(msg, ack, nack)
	})
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
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
		if err := join.handleEndOfRecordsMessage(clientID); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	join.handleDataMessage(fruitRecords, clientID)
}

func (join *Join) handleEndOfRecordsMessage(clientID uint32) error {
	slog.Info("Received End Of Records message")
	_, ok := join.endOfRegistersMap[clientID]
	if ok {
		join.endOfRegistersMap[clientID] = join.endOfRegistersMap[clientID] + 1
	} else {
		join.endOfRegistersMap[clientID] = 1
	}

	if join.endOfRegistersMap[clientID] == join.aggAmount {
		fruitTopRecords := join.buildFruitTop(clientID)
		message, err := inner.SerializeMessage(fruitTopRecords, clientID)
		if err != nil {
			slog.Debug("While serializing top message", "err", err)
			return err
		}
		if err := join.outputQueue.Send(*message); err != nil {
			slog.Debug("While sending top message", "err", err)
			return err
		}
	}
	return nil
}

func (join *Join) handleDataMessage(fruitRecords []fruititem.FruitItem, clientID uint32) {
	for _, fruitRecord := range fruitRecords {
		recordKey := recordkey.FruitKey{ClientID: clientID, FruitName: fruitRecord.Fruit}
		if _, ok := join.fruitItemMap[recordKey]; ok {
			join.fruitItemMap[recordKey] = join.fruitItemMap[recordKey].Sum(fruitRecord)
		} else {
			join.fruitItemMap[recordKey] = fruitRecord
		}
	}
}

func (join *Join) buildFruitTop(clientID uint32) []fruititem.FruitItem {
	fruitItems := make([]fruititem.FruitItem, 0, len(join.fruitItemMap))
	for key, item := range join.fruitItemMap {
		if key.ClientID == clientID {
			fruitItems = append(fruitItems, item)
		}
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(join.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}

func (join *Join) Close() {
	join.inputQueue.Close()
	join.outputQueue.Close()
}