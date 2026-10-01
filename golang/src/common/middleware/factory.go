package middleware

import (
	"fmt"
	"io"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

func newRMQConnection(connectionSettings ConnSettings) (*amqp.Connection, *amqp.Channel, error) {
	url := fmt.Sprintf("amqp://%s:%s@%s:%d/", "guest", "guest", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, nil, ErrMessageMiddlewareDisconnected
	}

	channel, err := conn.Channel()
	if err != nil {
		_ = CloseResources(conn)
		return nil, nil, ErrMessageMiddlewareDisconnected
	}

	return conn, channel, nil
}

type BaseMiddleware struct {
	myConnection  *amqp.Connection
	myChannel     *amqp.Channel
	myQueue       amqp.Queue
	myConsumerTag *SyncString
	consuming     bool
	mutex         sync.Mutex
}

func (bm *BaseMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	bm.mutex.Lock()
	if bm.consuming {
		bm.mutex.Unlock()
		return nil
	}
	bm.consuming = true
	msgs, err := bm.myChannel.Consume(
		bm.myQueue.Name,
		bm.myConsumerTag.Text(),
		false, false, false, false, nil,
	)
	if err != nil {
		bm.consuming = false
		bm.mutex.Unlock()
		return ErrMessageMiddlewareDisconnected
	}
	bm.mutex.Unlock()
	ConsumeFromQueue(msgs, callbackFunc, bm.myConsumerTag)
	bm.mutex.Lock()
	defer bm.mutex.Unlock()
	if bm.consuming {
		bm.consuming = false
		return ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (bm *BaseMiddleware) StopConsuming() error {
	bm.mutex.Lock()
	consumerTag := bm.myConsumerTag.Text()
	if !bm.consuming || consumerTag == "" {
		bm.mutex.Unlock()
		return nil
	}
	bm.consuming = false
	bm.mutex.Unlock()
	err := bm.myChannel.Cancel(consumerTag, false)
	if err != nil {
		return ErrMessageMiddlewareDisconnected
	}
	bm.myConsumerTag.Store("")
	return nil
}

func (bm *BaseMiddleware) Close() error {
	if err := bm.StopConsuming(); err != nil {
		return err
	}
	return CloseResources(bm.myChannel, bm.myConnection)
}

func ConsumeFromQueue(msgs <-chan amqp.Delivery, callbackFunc func(msg Message, ack func(), nack func()), tag *SyncString) {
	firstMessage := true
	for msg := range msgs {
		if firstMessage {
			tag.Store(msg.ConsumerTag)
		}
		firstMessage = false
		message := Message{Body: string(msg.Body)}
		ack := func() { _ = msg.Ack(false) }
		nack := func() { _ = msg.Nack(false, true) }
		callbackFunc(message, ack, nack)
	}
}

func CloseResources(resources ...io.Closer) error {
	var errors []error
	for _, resource := range resources {
		if resource == nil {
			continue
		}
		if err := resource.Close(); err != nil {
			errors = append(errors, err)
		}
	}
	if len(errors) > 0 {
		return ErrMessageMiddlewareClose
	}
	return nil
}

type SyncString struct {
	text string
	mu   sync.Mutex
}

func NewSyncString(text string) *SyncString {
	return &SyncString{text: text}
}

func (s *SyncString) Compare(text string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.text == text
}

func (s *SyncString) Text() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.text
}

func (s *SyncString) Store(newText string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.text = newText
}

type MyQueueMiddleware struct {
	myBaseMiddleware BaseMiddleware
}

func (mQ *MyQueueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	return mQ.myBaseMiddleware.StartConsuming(callbackFunc)
}

func (mQ *MyQueueMiddleware) StopConsuming() error {
	return mQ.myBaseMiddleware.StopConsuming()
}

func (mQ *MyQueueMiddleware) Send(msg Message) error {
	err := mQ.myBaseMiddleware.myChannel.Publish("", mQ.myBaseMiddleware.myQueue.Name, false, false, amqp.Publishing{ContentType: "text/plain", Body: []byte(msg.Body)})
	if err != nil {
		return ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (mQ *MyQueueMiddleware) SendWithKeys(msg Message, _key []string) error {
	return mQ.Send(msg)
}

func (mQ *MyQueueMiddleware) Close() error {
	return mQ.myBaseMiddleware.Close()
}

func CreateQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
	conn, channel, err := newRMQConnection(connectionSettings)
	if err != nil {
		return nil, err
	}
	err = channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		er := CloseResources(channel, conn)
		if er != nil {
			return nil, er
		}
		return nil, ErrMessageMiddlewareDisconnected
	}
	queue, err := channel.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		er := CloseResources(channel, conn)
		if er != nil {
			return nil, er
		}
		return nil, ErrMessageMiddlewareDisconnected
	}
	aMiddleWare := &MyQueueMiddleware{
		myBaseMiddleware: BaseMiddleware{
			myConnection:  conn,
			myChannel:     channel,
			myQueue:       queue,
			myConsumerTag: NewSyncString(""),
		},
	}
	return aMiddleWare, nil
}

type MyExchangeMiddleware struct {
	myBaseMiddleware BaseMiddleware
	myExchangeName   string
	myKeys           []string
}

func (mE *MyExchangeMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	return mE.myBaseMiddleware.StartConsuming(callbackFunc)
}

func (mE *MyExchangeMiddleware) StopConsuming() error {
	return mE.myBaseMiddleware.StopConsuming()
}

func (mE *MyExchangeMiddleware) Send(msg Message) error {
	return mE.SendWithKeys(msg, mE.myKeys)
}

func (mE *MyExchangeMiddleware) SendWithKeys(msg Message, keys []string) error {
	for _, key := range keys {
		err := mE.myBaseMiddleware.myChannel.Publish(mE.myExchangeName, key, false, false, amqp.Publishing{ContentType: "text/plain", Body: []byte(msg.Body)})
		if err != nil {
			return ErrMessageMiddlewareDisconnected
		}
	}
	return nil
}

func (mE *MyExchangeMiddleware) Close() error {
	return mE.myBaseMiddleware.Close()
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings ConnSettings) (Middleware, error) {
	conn, channel, err := newRMQConnection(connectionSettings)
	if err != nil {
		return nil, err
	}
	err = channel.ExchangeDeclare(
		exchange,
		"direct",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		er := CloseResources(channel, conn)
		if er != nil {
			return nil, er
		}
		return nil, ErrMessageMiddlewareDisconnected
	}
	queue, err := channel.QueueDeclare(
		"",
		false,
		true,
		true,
		false,
		nil,
	)
	if err != nil {
		er := CloseResources(channel, conn)
		if er != nil {
			return nil, er
		}
		return nil, ErrMessageMiddlewareDisconnected
	}
	for _, key := range keys {
		err = channel.QueueBind(queue.Name, key, exchange, false, nil)
		if err != nil {
			er := CloseResources(channel, conn)
			if er != nil {
				return nil, er
			}
			return nil, ErrMessageMiddlewareMessage
		}
	}
	aMiddleware := &MyExchangeMiddleware{
		myBaseMiddleware: BaseMiddleware{
			myConnection:  conn,
			myChannel:     channel,
			myQueue:       queue,
			myConsumerTag: NewSyncString(""),
		},
		myExchangeName: exchange,
		myKeys:         keys,
	}
	return aMiddleware, nil
}
