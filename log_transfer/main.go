package main

import (
	"fmt"

	"github.com/jstang007/gateway_demo/study/33log_transfer/conf"
	"github.com/jstang007/gateway_demo/study/33log_transfer/es"
	"github.com/jstang007/gateway_demo/study/33log_transfer/kafka"
	"gopkg.in/ini.v1"
)

//log transfer
//将日志数据从kafka取出来发往ES

func main() {
	//---------------------------------------------0.加载配置文件-----------------------------------------------
	var cfg conf.LogTransferCfg
	if err := ini.MapTo(&cfg, "./conf/cfg.ini"); err != nil {
		fmt.Println("Init config failed, err: ", err)
		return
	}
	fmt.Println("Load config success. ", cfg)

	//----------------------------------------------1.初始化ES--------------------------------------------------
	//1.1创建一个ES客户端
	//1.2创建从kafka取日志数据后台进程
	if err := es.Init(cfg.ESCfg.Address, cfg.ESCfg.ChanSize, cfg.ESCfg.Nums); err != nil {
		fmt.Println("Init ES failed. err: ", err)
		return
	}
	fmt.Println("Init ES success. ")
	//----------------------------------------------2.初始化kafka----------------------------------------------------
	//2.1连接kafka，创建分区消费者
	//2.2每个分区的消费者分别取出数据，通过SendToES()发往ES的后台进程
	if err := kafka.Init([]string{cfg.KafkaCfg.Address}, cfg.KafkaCfg.Topic); err != nil {
		fmt.Println("init kafka consumer failed, err: ", err)
		return
	}
	fmt.Println("Init kafka consumer success. ")
	select {}
}
