package taillog

import (
	"context"
	"fmt"

	"github.com/hpcloud/tail"
	"github.com/jstang9527/logagent/kafka"
)

//专门从日志文件收集日志的模块

//TailTask 日志收集任务对象
type TailTask struct {
	path     string
	topic    string
	instance *tail.Tail
	//为了实现退出t.run()
	ctx        context.Context
	cancelFunc context.CancelFunc
}

//NewTailTask 日志收集对象
func NewTailTask(path, topic string) (tailObj *TailTask) {
	ctx, cancel := context.WithCancel(context.Background())
	tailObj = &TailTask{
		path:       path,
		topic:      topic,
		ctx:        ctx,
		cancelFunc: cancel,
	}
	tailObj.init() //根据路径打开对应的日志
	return
}

//Init ...
func (t *TailTask) init() { //不带指针的话，只读t
	config := tail.Config{
		ReOpen:    true,
		Follow:    true,
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2},
		MustExist: false,
		Poll:      true,
	}
	var err error
	t.instance, err = tail.TailFile(t.path, config) //只读t执行此句不会产生效果
	if err != nil {
		fmt.Println("tail file failed, err: ", err)
	}
	//当goroutine执行函数退出时，goroutine就结束了
	go t.run() //直接采集日志发送到kafka
}

//Run ...
func (t *TailTask) run() {
	for {
		select {
		case <-t.ctx.Done():
			fmt.Printf("tail task is finished. [topic:%s, path:%s]\n", t.topic, t.path)
			return
		case line := <-t.instance.Lines: //从tailobj的通道中一行行读取日志文件
			//3.2发往kafka
			//kafka.SendToKafka(t.topic, line.Text) //读一条存一条？改用👇方式
			//把日志数据发送到通道，从通道取值
			kafka.SendToChan(t.topic, line.Text)
		}
	}
}
