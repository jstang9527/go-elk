package es

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/olivere/elastic"
)

var (
	client *elastic.Client
	ch     chan *LogData
)

//LogData ...
type LogData struct {
	Topic string `json:"topic"`
	Data  string `json:"data"`
}

//Init 初始化ES，准备接收kafka发来的数据
func Init(address string, chanSize, nums int) (err error) {
	if !strings.HasPrefix(address, "http://") {
		address = "http://" + address
	}
	client, err = elastic.NewClient(elastic.SetURL(address))
	if err != nil {
		fmt.Println("create es client failed, err: ", err)
		return
	}
	fmt.Println("create es client success.")
	ch = make(chan *LogData, chanSize)
	for i := 0; i < nums; i++ {
		go SendToES()
	}
	fmt.Printf("create [%d] daemon service of send data to ES.\n", nums)
	return
}

// SendToESChan ...
func SendToESChan(msg *LogData) {
	ch <- msg
}

// SendToES 发送数据到ES
func SendToES() { //数据库indexStr、表typeStr、值data
	// put1, err := client.Index().Index(indexStr).Type(typeStr).BodyJson(data).Do(context.Background())
	for {
		select {
		case msg := <-ch:
			put1, err := client.Index().Index(msg.Topic).Type("xxx").BodyJson(msg).Do(context.Background()) //BodyJson必须为结构体
			if err != nil {
				fmt.Println(err)
			}
			fmt.Printf("indexed %s %s to index %s, type %s, data->%v\n", msg.Topic, put1.Id, put1.Index, put1.Type, msg.Data)
		default:
			time.Sleep(time.Second)
		}
	}

}
