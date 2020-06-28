package conf

//AppConf ...
type AppConf struct {
	KafkaConf `ini:"kafka"`
	// TaillogConf `ini:"taillog"`
	EtcdConf `ini:"etcd"`
}

// KafkaConf ...
type KafkaConf struct {
	Address     string `ini:"address"`
	ChanMaxSize int    `ini:"chan_max_size"`
	// Topic   string `ini:"topic"`
}

// EtcdConf ...
type EtcdConf struct {
	Address string `ini:"address"`
	Timeout int    `ini:"timeout"`
	Key     string `ini:"collect_log_key"`
}

//TaillogConf ...
type TaillogConf struct {
	FileName string `ini:"filename"`
}
