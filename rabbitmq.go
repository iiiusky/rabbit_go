/*
Copyright © 2022 iiusky sky@03sec.com

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package rabbit_go

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type RabbitMQ struct {
	exchange         string
	exchangeType     string
	routingKey       string
	amqpURI          string
	reTryTime        time.Duration
	handleMsg        func(d amqp.Delivery) bool
	forever          chan bool
	queueName        string
	consumeName      string
	mutex            *sync.Mutex
	connections      map[int]*amqp.Connection
	channels         map[int]channel
	busyChannels     map[int]int
	MaxConnectionNum int
	MaxChannelNum    int
	idelChannels     []int
}

const (
	waitConfirmTime = 3 * time.Second
)

type channel struct {
	ch            *amqp.Channel
	notifyClose   chan *amqp.Error
	notifyConfirm chan amqp.Confirmation
}

func NewRabbitMQ(amqpURI, exchange, exchangeType, routingKey string, queueName string,
	reTryTime time.Duration, daemon bool, maxConnectionNum, maxChannelNum int) (*RabbitMQ, error) {
	c := &RabbitMQ{
		exchange:         exchange,
		exchangeType:     exchangeType,
		routingKey:       routingKey,
		reTryTime:        reTryTime,
		amqpURI:          amqpURI,
		queueName:        queueName,
		mutex:            new(sync.Mutex),
		busyChannels:     make(map[int]int),
		MaxConnectionNum: maxConnectionNum,
		MaxChannelNum:    maxChannelNum,
	}
	c.connections = make(map[int]*amqp.Connection)
	c.createConnectionPool()
	c.createChannelPool()

	if daemon {
		go c.Daemon()
	}

	return c, nil
}

func (r *RabbitMQ) createConnectionPool() {
	r.connections = make(map[int]*amqp.Connection)
	for i := 0; i < r.MaxConnectionNum; i++ {
		conn := r.connect()
		if conn != nil {
			r.connections[i] = conn
		}
	}
}

func (r *RabbitMQ) connect() *amqp.Connection {
	var err error
	conn, err := amqp.Dial(r.amqpURI)
	if err != nil {
		fmt.Errorf("AMQP Dial Error %v", err)
		return nil
	}
	return conn
}

func (r *RabbitMQ) createChannelPool() {
	r.channels = make(map[int]channel)
	for index, connection := range r.connections {
		for j := 0; j < r.MaxChannelNum; j++ {
			key := index*r.MaxChannelNum + j
			r.channels[key] = r.createChannel(index, connection)
			r.idelChannels = append(r.idelChannels, key)
		}
	}
}

func (r *RabbitMQ) createChannel(connectId int, conn *amqp.Connection) channel {
	var notifyClose = make(chan *amqp.Error)
	var notifyConfirm = make(chan amqp.Confirmation)

	cha := channel{
		notifyClose:   notifyClose,
		notifyConfirm: notifyConfirm,
	}
	if conn.IsClosed() {
		conn = r.connect()
	}
	ch, err := conn.Channel()
	if err != nil {
		ch = r.recreateChannel(connectId, err)
	}

	ch.Confirm(false)
	ch.NotifyClose(cha.notifyClose)
	ch.NotifyPublish(cha.notifyConfirm)

	go func() {
		select {
		case <-cha.notifyClose:
			fmt.Println("close channel")
		}
	}()

	if err = ch.ExchangeDeclare(
		r.exchange,     // name of the exchange
		r.exchangeType, // type
		true,           // durable
		false,          // delete when complete
		false,          // internal
		false,          // noWait
		nil,            // arguments
	); err != nil {
		fmt.Errorf("declared Exchange %s", err)
		return cha
	}

	fmt.Println(fmt.Sprintf("declared Exchange, declaring Queue %q", r.queueName))
	_, err = ch.QueueDeclare(
		r.queueName, // name of the queue
		true,        // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // noWait
		nil,         // arguments
	)
	if err != nil {
		fmt.Errorf("Queue Declare: %s", err)
		return cha
	}

	if err = ch.QueueBind(
		r.queueName,  // name of the queue
		r.routingKey, // bindingKey
		r.exchange,   // sourceExchange
		false,        // noWait
		nil,          // arguments
	); err != nil {
		fmt.Errorf("Queue Bind: %s", err)
		return cha
	}

	cha.ch = ch
	return cha
}

func (r *RabbitMQ) recreateChannel(connectId int, err error) (ch *amqp.Channel) {
	if strings.Index(err.Error(), "channel/connection is not open") >= 0 || strings.Index(err.Error(), "CHANNEL_ERROR - expected 'channel.open'") >= 0 {
		//S.connections[connectId].Close()
		var newConn *amqp.Connection
		if r.connections[connectId].IsClosed() {
			newConn = r.connect()
		} else {
			newConn = r.connections[connectId]
		}
		r.lockWriteConnect(connectId, newConn)
		//S.connections[connectId] = newConn
		ch, err = newConn.Channel()
		failOnError(err, "Failed to open a channel")
	} else {
		failOnError(err, "Failed to open a channel")
	}
	return
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func (r *RabbitMQ) lockWriteConnect(connectId int, newConn *amqp.Connection) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.connections[connectId] = newConn
}

func (r *RabbitMQ) getChannel() (*amqp.Channel, int) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	idelLength := len(r.idelChannels)
	if idelLength > 0 {
		rand.Seed(time.Now().Unix())
		index := rand.Intn(idelLength)
		channelId := r.idelChannels[index]
		r.idelChannels = append(r.idelChannels[:index], r.idelChannels[index+1:]...)
		r.busyChannels[channelId] = channelId

		ch := r.channels[channelId].ch
		return ch, channelId
	} else {
		//return S.createChannel(0,S.connections[0]),-1
		return nil, -1
	}
}

func (r *RabbitMQ) lockWriteChannel(channelId int, cha channel) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.channels[channelId] = cha
}

func (r *RabbitMQ) Publish(data string) (err error) {
	ch, channelId := r.getChannel()
	cha := channel{}
	if ch == nil {
		cha = r.createChannel(0, r.connections[0])
		defer cha.ch.Close()
		ch = cha.ch
		fmt.Println("ch: ", ch)
	}
	err = ch.Publish(
		r.exchange,   // exchange
		r.routingKey, //severityFrom(os.Args), // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			Headers:      amqp.Table{},
			ContentType:  "text/plain",
			DeliveryMode: amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:     0,              // 0-9
			Timestamp:    time.Time{},
			Body:         []byte(data),
		})

	if err != nil {
		if strings.Index(err.Error(), "channel/connection is not open") >= 0 {
			fmt.Println("channel/connection is not open")
		}
		fmt.Println("err: ", err)
	}

	select {
	case confirm := <-r.channels[channelId].notifyConfirm:
		if confirm.Ack {
			r.backChannelId(channelId, ch)
			log.Printf(" [%s] Sent %d message %s success", r.routingKey, confirm.DeliveryTag, data)
			return nil
		}
		return errors.New("ack failed")
	case chaConfirm := <-cha.notifyConfirm:
		if chaConfirm.Ack {
			return nil
		}
		return errors.New("ack failed")
	case <-time.After(waitConfirmTime):
		log.Printf("message: %s data: %s", "Can not receive the confirm.", data)
		r.backChannelId(channelId, ch)
		confirmErr := errors.New("Can not receive the confirm . ")
		return confirmErr
	}
}

func (r *RabbitMQ) backChannelId(channelId int, ch *amqp.Channel) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.idelChannels = append(r.idelChannels, channelId)
	delete(r.busyChannels, channelId)
	return
}

func (r *RabbitMQ) publishConfirmOne(confirms <-chan amqp.Confirmation, ch *amqp.Channel) {
	fmt.Println("waiting for confirmation of one publishing")

	if confirmed := <-confirms; confirmed.Ack {
		fmt.Println(fmt.Sprintf("confirmed delivery with delivery tag: %d", confirmed.DeliveryTag))
	} else {
		fmt.Errorf("failed delivery of delivery tag: %d", confirmed.DeliveryTag)
	}
	ch.Close()
}

func (r *RabbitMQ) Daemon() {
	for {
		for index := 0; index < r.MaxConnectionNum; index++ {
			conn := r.connections[index]
			if conn.IsClosed() {
				time.Sleep(r.reTryTime)

				conn = r.connect()
				if conn != nil {
					r.connections[index] = conn
				}

				for j := 0; j < r.MaxChannelNum; j++ {
					key := index*r.MaxChannelNum + j
					r.channels[key] = r.createChannel(index, conn)
					r.idelChannels = append(r.idelChannels, key)
				}
			}
		}
	}
}

func (r *RabbitMQ) StartConsumer(handleMsg func(d amqp.Delivery) bool, deliveries <-chan amqp.Delivery) {
	r.forever = make(chan bool)
	r.handleMsg = handleMsg
	// 启用协程处理消息
	go func() {
		for d := range deliveries {
			status := handleMsg(d)
			ackErr := d.Ack(!status)
			if ackErr != nil {
				fmt.Errorf("设置ack失败 %v", ackErr)
			}
		}
	}()
	fmt.Println(" [*] Waiting for messages. To exit press CTRL+C")
	<-r.forever
}

func (r *RabbitMQ) NewConsumer(queueName, consumeName string, maxConsumeNum int, handleMsg func(d amqp.Delivery) bool) {
	r.forever = make(chan bool)

	if maxConsumeNum > (r.MaxConnectionNum * r.MaxChannelNum) {
		panic("maxConsumeNum is too large")
	}

	for i := 0; i < maxConsumeNum; i++ {
		r.queueName = queueName
		r.consumeName = consumeName
		ch, _ := r.getChannel()
		fmt.Println(fmt.Sprintf("got Channel, declaring Exchange (%q)", r.exchange))
		deliveries, err := ch.Consume(
			queueName,                              // name
			fmt.Sprintf("%s - %d", consumeName, i), // consumeName,
			false,                                  // noAck
			false,                                  // exclusive
			false,                                  // noLocal
			false,                                  // noWait
			nil,                                    // arguments
		)
		if err != nil {
			fmt.Errorf("Queue Consume: %s", err)
			continue
		}
		go r.StartConsumer(handleMsg, deliveries)
	}
	<-r.forever
}
