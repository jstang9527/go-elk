package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.etcd.io/etcd/clientv3"
)

var (
	cli *clientv3.Client
)

//LogEntry 需要收集的日志的配置信息
type LogEntry struct {
	Path  string `json:"path"`  //日志存放路径
	Topic string `json:"topic"` //日志要发往kafka中哪个Topic
}

// Init 谁会用etcd呢？谁想从etcd拿数据？
func Init(addr string, timeout time.Duration) (err error) {
	cli, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{addr},
		DialTimeout: timeout,
	})
	return
}

//GetConf 从etcd中根据key获取配置项
func GetConf(key string) (logEntryConf []*LogEntry, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := cli.Get(ctx, key)
	cancel()
	if err != nil {
		fmt.Println("get from etcd failed, err:", err)
		return
	}
	for _, ev := range resp.Kvs {
		// 把json数据转结构体，因为存进etcd的值也是json。
		err = json.Unmarshal(ev.Value, &logEntryConf)
		if err != nil {
			fmt.Println("unmarshal etcd value failed, err:", err)
			return
		}
	}
	return
}

//WatchConf 监测etcd中对应key配置信息的更改, 若发生更改则向tailMgr报告(往它的写入通道塞值)
func WatchConf(key string, newConfCh chan<- []*LogEntry) {
	ch := cli.Watch(context.Background(), key)
	//通道尝试获取值
	for wresp := range ch {
		for _, evt := range wresp.Events {
			//通知其他人
			//1.先判断操作的类型, 为啥？因为当配置被删除时,json(字符串)的零值是不能转结构体的
			var newConf []*LogEntry
			if evt.Type != clientv3.EventTypeDelete {
				if err := json.Unmarshal(evt.Kv.Value, &newConf); err != nil {
					fmt.Println("unmarshal failed, err:", err)
					continue
				}
			}
			fmt.Println("get new conf:", newConf)
			newConfCh <- newConf
		}
	}
}
