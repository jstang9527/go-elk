package kafka

import (
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/jstang007/gateway_demo/study/33log_transfer/es"
)

//Init 创建消费者，消费kafka数据发往ES
func Init(address []string, topic string) (err error) {
	consumer, err := sarama.NewConsumer(address, nil)
	if err != nil {
		fmt.Println("fail to start consumer, err: ", err)
		return
	}
	fmt.Println("create consumer success.")
	partitonList, err := consumer.Partitions(topic) //根据topic取到所有分区
	if err != nil {
		fmt.Println("fail to get list of partition, err: ", err)
		return
	}
	fmt.Println("partition list: ", partitonList)
	for partition := range partitonList { //遍历所有分区
		//针对每个分区创建一个对应的分区消费者
		pc, err := consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			fmt.Println("failed to start consumer for partition ", partition, "err: ", err)
			return err
		}
		// defer pc.AsyncClose()
		// 异步消费信息
		go func(sarama.PartitionConsumer) {
			for msg := range pc.Messages() {
				fmt.Printf("Partition:%d Offset:%d Key:%v Value:%s\n", msg.Partition, msg.Offset, msg.Key, string(msg.Value))
				//直接发给es
				ld := es.LogData{Topic: topic, Data: string(msg.Value)}
				es.SendToESChan(&ld)
			}
		}(pc)
	}
	return
}
