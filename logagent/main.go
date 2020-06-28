package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/jstang9527/logagent/conf"
	"github.com/jstang9527/logagent/etcd"
	"github.com/jstang9527/logagent/kafka"
	"github.com/jstang9527/logagent/taillog"
	"github.com/jstang9527/logagent/utils"
	"gopkg.in/ini.v1"
)

var cfg = new(conf.AppConf)

func main() {
	//--------------------------------------0.加载配置文件--------------------------------------
	// cfg,err:= ini.Load("./conf/config.ini")
	// fmt.Println(cfg.Section("kafka").Key("address")
	err := ini.MapTo(cfg, "./conf/config.ini") //mapto映射结构体
	if err != nil {
		fmt.Println("load ini failed, err: ", err)
		return
	}
	fmt.Println("check validity of tail tools from ini file: ", cfg.KafkaConf.Address, cfg.EtcdConf.Address, cfg.EtcdConf.Timeout)

	//-----------------1.初始化kafka连接(这个还是从文件读取，后期使用etcd拿数据)---------------------
	if err := kafka.Init([]string{cfg.KafkaConf.Address}, cfg.KafkaConf.ChanMaxSize); err != nil {
		fmt.Println("init kafka failed, err: ", err)
		return
	}
	fmt.Println("init kafka success.")

	//-----------------------------------2.初始化etcd, 拿日志配置项数据---------------------------------
	err = etcd.Init(cfg.EtcdConf.Address, time.Duration(cfg.EtcdConf.Timeout)*time.Second)
	if err != nil {
		fmt.Println("init etcd failed, err:", err)
		return
	}
	fmt.Println("init ectd success.")
	//为了实现每个logagent都拉取自己独有的配置，所以要以自己的IP地址作为区分(也可用业务线区分)
	ipStr, err := utils.GetLocalIP()
	if err != nil {
		panic(err)
	}
	etcdConfKey := fmt.Sprintf(cfg.EtcdConf.Key, ipStr)
	//2.1从etcd获取日志收集项的配置信息结构体->logEntryConf
	logEntryConf, err := etcd.GetConf(etcdConfKey)
	if err != nil {
		fmt.Println("etcd.GetConf failed, err:", err)
		return
	}
	fmt.Println("get conf from etcd success, ", logEntryConf)
	//【test】打印一下从etcd获取到配置项的值
	for index, value := range logEntryConf {
		fmt.Println("item: ", index, ", -> ", value)
	}

	//---------------------------------3.tail工具实例化，并热加载日志项数据------------------------------
	//3.1传入日志配置项结构体列表, 会发生什么事？
	// * 将实例化管理者进行管理日志配置项结构体列表
	// * 给每个日志配置项分配专属tail工具对象(创建对象后自动创建读日志的后台程序)
	// * 监听etcd的日志配置的变更，实现热加载(对删除的给tail发送指令停止读日志的后台进程)
	taillog.Init(logEntryConf)

	//3.2把通知通道拿出来, 在3.3中,给etcd塞值(日志更新后的日志配置项值)
	newConfChan := taillog.NewConfChan()

	//3.3派一个哨兵(后台进程)去监视日志收集项目的配置变化, 给通知通道塞值来通知taillog模板更新配置。
	var wg sync.WaitGroup
	wg.Add(1)
	go etcd.WatchConf(etcdConfKey, newConfChan) //哨兵发现配置更新会向taillog(通道)通知
	wg.Wait()

	//--------------------------------------↓↓↓ 4.具体业务 ↓↓↓------------------------------------
	// run()
}
