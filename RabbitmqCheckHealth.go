package main

import (
	"fmt"
	"log"
	"os"
	"time"

	zabbix "github.com/adubkov/go-zabbix"
	"github.com/streadway/amqp"
	"gopkg.in/ini.v1"
)

const (
	senderPort = 10051
)

func readCfg() (cfg *ini.File) {
	cfg, err := ini.Load(os.Args[1])
	if err != nil {
		log.Printf("Fail to read file:", err)
	}
	return cfg
}

func conn_mq() (conn *amqp.Connection) {
	cfg := readCfg()
	section := "rabbitmq"
	mq_user := cfg.Section(section).Key("Username").String()
	mq_passd := cfg.Section(section).Key("Password").String()
	MqHost := cfg.Section(section).Key("MqHost").String()
	MqPort := cfg.Section(section).Key("MqPort").String()
	MqVhosts := cfg.Section(section).Key("MqVhosts").String()
	mq_url := fmt.Sprintf("amqp://%s:%s@%s:%s/%s", mq_user, mq_passd, MqHost, MqPort, MqVhosts)
	for {
		conn, err := amqp.Dial(mq_url)
		if err != nil {
			senders := fmt.Sprintf("Rabbitmq Service Unavailable. [ERROR] %v", err)
			log.Printf("Failed to connect to RabbitMQ: %v", err)
			zabbix_sender(senders)
			time.Sleep(2 * time.Second)
		} else {
			return conn
			break
		}
	}
	return
}

func mq_producer(chan1 chan bool) {
RET:
	conn := conn_mq()
	defer conn.Close()
	ch, err := conn.Channel()
	senders_channel := fmt.Sprintf("Rabbitmq Service Unavailable. [ERROR] %v", err)
	if err != nil {
		zabbix_sender(senders_channel)
		log.Printf("[producer] Failed to open a channel: %v", err)
	}
	defer ch.Close()

	cfg := readCfg()
	queueName := cfg.Section("rabbitmq").Key("QueueName").String()
	exchangeName := cfg.Section("rabbitmq").Key("exchangeName").String()
	queue, err := ch.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	senders_queue := fmt.Sprintf("Rabbitmq Service Unavailable. [ERROR] %v", err)
	if err != nil {
		zabbix_sender(senders_queue)
		log.Printf("[producer] Failed to declare a queue: %v", err)
	}

	//绑定exchange
	err = ch.QueueBind(queueName, queue.Name, exchangeName, false, nil)
	if err != nil {
		senders_bind := fmt.Sprintf("Rabbitmq Service Unavailable. [ERROR] %v", err)
		zabbix_sender(senders_bind)
		log.Printf("Failed to queuebinds a message: %v", err)
	}

	body := "Rabbitmq Service OK!"
	for {
		err = ch.Publish(
			exchangeName, //exchange
			queue.Name,   // routing key
			false,        // mandatory
			false,        // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
			})
		senders_publish := fmt.Sprintf("Rabbitmq Service Unavailable. [ERROR] %v", err)
		if err != nil {
			zabbix_sender(senders_publish)
			log.Printf("Failed to publish a message: %v", err)
			goto RET
		} else {
			log.Printf("Publish: %s", body)
		}
		time.Sleep(time.Duration(1) * time.Second)
	}
	chan1 <- true
}

func mq_consume(chan2 chan bool) {
RET:
	conn := conn_mq()
	defer conn.Close()

	ch, err := conn.Channel()
	senders_channel := fmt.Sprintf("Rabbitmq Service Unavailable. [ERROR] %v", err)
	if err != nil {
		zabbix_sender(senders_channel)
		log.Printf("[consume] Failed to open a channel: %v", err)
	}
	defer ch.Close()

	cfg := readCfg()
	queueName := cfg.Section("rabbitmq").Key("QueueName").String()
	q, err := ch.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	senders_queue := fmt.Sprintf("Rabbitmq Service Unavailable. [ERROR] %v", err)
	if err != nil {
		zabbix_sender(senders_queue)
		log.Printf("[consume] Failed to declare a queue: %v", err)
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	senders_register := fmt.Sprintf("Rabbitmq Service Unavailable. [ERROR] %v", err)
	if err != nil {
		zabbix_sender(senders_register)
		log.Printf("[consume]  Failed to register a consumer: %v", err)
		goto RET
	}
	forever := make(chan bool)
	timers := time.NewTimer(time.Second * 60)
	time2 := time.NewTimer(time.Second * 60)
	go func() {
		send := make(chan string) //, 2)
		for d := range msgs {
			senders := string(d.Body)
			log.Printf("Received a message: %s", senders)
			go func() {
				<-time2.C
				send <- senders
				time2.Reset(time.Second * 60)
			}()
			go func() {
				for {
					<-timers.C
					info := <-send
					log.Printf("从通道中获取的值为: %s", info)
					log.Printf("开始上传数据到ZABBIX! ->%s", info)
					zabbix_sender(info)
					log.Printf("数据上传完成！！")
					timers.Reset(time.Second * 60)
				}
			}()
		}
	}()
	<-forever
	chan2 <- true
}

func zabbix_sender(sendValue string) {
	cfg := readCfg()
	ZabbixServerHost := cfg.Section("zabbix").Key("ZabbixServerHost").String()
	ZabbixAgentHost := cfg.Section("zabbix").Key("ZabbixAgentHost").String()
	Zabbix_Key := cfg.Section("zabbix").Key("Zabbix_Key").String()

	var metrics []*zabbix.Metric
	metrics = append(metrics, zabbix.NewMetric(ZabbixAgentHost, Zabbix_Key, sendValue))

	// Create instance of Packet class
	packet := zabbix.NewPacket(metrics)

	// Send packet to zabbix
	z := zabbix.NewSender(ZabbixServerHost, senderPort)
	z.Send(packet)

}

func main() {
	args := os.Args
	if args == nil || len(args) != 2 || len(args[1]) == 0 {
		fmt.Printf(`请在当前目录下创建*.ini(配置文件名自定义)配置文件,文件格式为:
			[zabbix]
			zabbix_hosts = 192.168.137.24;
			启动:%s CfgFileName`, args[0])
		return
	}
	ch1, ch2 := make(chan bool), make(chan bool)
	go mq_producer(ch1)
	time.Sleep(1 * time.Second)
	go mq_consume(ch2)
	<-ch1
	<-ch2
}
