package taillog

import (
	"fmt"
	"time"

	"github.com/jstang9527/logagent/etcd"
)

var taskMgr *tailLogMgr

//管理存储日志对象
type tailLogMgr struct {
	logEntry    []*etcd.LogEntry      //已有的日志项信息
	taskMap     map[string]*TailTask  //
	newConfChan chan []*etcd.LogEntry //接收更改日志项配置的通道
}

// Init ...
func Init(logEntryConf []*etcd.LogEntry) {
	//管理日志对象
	taskMgr = &tailLogMgr{
		logEntry:    logEntryConf,
		taskMap:     make(map[string]*TailTask, 16),
		newConfChan: make(chan []*etcd.LogEntry), //无缓冲区
	}
	//对每个日志对象实例化专属的tail工具对象，存储管理者中taskMap中
	for _, logEntry := range logEntryConf {
		tailObj := NewTailTask(logEntry.Path, logEntry.Topic)
		mk := fmt.Sprintf("%s_%s", logEntry.Path, logEntry.Topic)
		taskMgr.taskMap[mk] = tailObj
	}
	go taskMgr.run()
}

//管理者监听newConfChan, 监听配置更改(add,del,put)
func (t *tailLogMgr) run() {
	for {
		select {
		case newConf := <-t.newConfChan:
			for _, conf := range newConf { //每项每项拿数据
				mk := fmt.Sprintf("%s_%s", conf.Path, conf.Topic)
				if _, ok := t.taskMap[mk]; ok {
					continue //原来就有，不需要操作
				} else {
					tailObj := NewTailTask(conf.Path, conf.Topic)
					t.taskMap[mk] = tailObj
				}
			}
			//找出原来t.logEntry有, 但是newConf中没有的，要删掉，停掉对应的tail工具对象
			for index, c1 := range t.logEntry {
				fmt.Println("No.", index, "Member: ", c1.Path, " - ", c1.Topic)
				isDel := true
				for _, c2 := range newConf {
					// fmt.Println("------=>", c1.Path, c1.Topic, " | ", c2.Path, c2.Topic)
					if c2.Path == c1.Path && c2.Topic == c1.Topic { //原来跟现在的对上
						isDel = false
						break
					}
				}
				if isDel { //把对应的tail工具停掉
					mk := fmt.Sprintf("%s_%s", c1.Path, c1.Topic)
					t.taskMap[mk].cancelFunc()
				}
			}
			fmt.Println("log objtect has been updated by etcd watch.", newConf)
			t.logEntry = newConf //将旧成员替换为新成员
		default:
			time.Sleep(time.Second)
		}
	}
}

//NewConfChan 项外暴露通知通道
func NewConfChan() chan<- []*etcd.LogEntry {
	return taskMgr.newConfChan
}
