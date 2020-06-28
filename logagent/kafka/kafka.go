package kafka

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

//专门往kafka写日志的模块

type logData struct {
	topic string
	data  string
}

var (
	client      sarama.SyncProducer //声明全局连接kafka生产者client
	logDataChan chan *logData
)

//Init ...
func Init(address []string, maxSize int) (err error) {
	config := sarama.NewConfig()
	//tailf包使用
	config.Producer.RequiredAcks = sarama.WaitForAll          //发生哇数据需要leader和follow都确认
	config.Producer.Partitioner = sarama.NewRandomPartitioner //新选出一个partition
	config.Producer.Return.Successes = true                   //成功交付的消息将在success channel返回

	//连接kafka
	client, err = sarama.NewSyncProducer(address, config)
	if err != nil {
		fmt.Println("producer closed, err: ", err)
		return
	}
	logDataChan = make(chan *logData, maxSize)
	//开启后台输送kafka数据的进程
	go sendToKafka()
	return
}

// SendToChan 暴露给别人往通道中塞数据
func SendToChan(topic, data string) {
	msg := &logData{
		topic: topic,
		data:  data,
	}
	logDataChan <- msg
}

//SendToKafka 真正往kafka发送日志的函数
func sendToKafka() {
	for {
		select {
		case ld := <-logDataChan:
			//包装信息
			msg := &sarama.ProducerMessage{}
			msg.Topic = ld.topic
			msg.Value = sarama.StringEncoder(ld.data)

			//发送到kafka
			pid, offset, err := client.SendMessage(msg)
			if err != nil {
				fmt.Println("send msg failed, err: ", err)
				return
			}
			fmt.Printf("pid:%v, offset:%v\n", pid, offset)

		default:
			time.Sleep(time.Millisecond * 50)
		}
	}
}
