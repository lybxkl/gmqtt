package config

type Broker struct {
	MqttAddr      string `toml:"mqttAddr" validate:"default=tcp://:1883"`
	MqttTLSOpen   bool   `toml:"mqttTlsOpen"`
	WsAddr        string `toml:"wsAddr" validate:"default=:8765"`
	WsPath        string `toml:"wsPath" validate:"default=/mqtt"`
	WssAddr       string `toml:"wssAddr"`
	WssCertPath   string `toml:"wssCertPath"`
	WssKeyPath    string `toml:"wssKeyPath"`
	Ca            string `toml:"ca"`
	CloseShareSub bool   `toml:"closeShareSub"`

	ServerTaskPoolSize      int `toml:"server_task_pool_size" validate:"default=2000"`
	GettyServerTaskPoolSize int `toml:"getty_server_task_pool_size" validate:"default=10000"`

	AllowZeroLengthClientId    bool   `toml:"allow_zero_length_clientId"`
	MaxQos                     int    `toml:"max_qos"  validate:"default=2"`                        // 支持的最大qos，默认2，不得低于1
	AutoIdPrefix               string `toml:"auto_id_prefix" validate:"default=auto-"`              // 设置客户端id前缀， 默认auto-
	PerListenerSettings        bool   `toml:"per_listener_settings"`                                // 每个listener都需要配置参数
	CheckRetainSource          bool   `toml:"check_retain_source"`                                  // 检查发送者是否有发送保留消息的权限，有则可以发送
	MaxInflightBytes           int32  `toml:"max_inflight_bytes" validate:"default=0"`              // 对于Qos1/2, 表示最大正在处理的字节数，默认为0（无最大值）
	MaxInflightMessages        int32  `toml:"max_inflight_messages" validate:"default=1000"`        // 同时传输的消息最大数， qos>0
	MaxKeepalive               uint16 `toml:"max_keepalive" validate:"default=300"`                 // 允许最大值65535 单位为s, 默认5分钟
	MaxPacketSize              uint32 `toml:"max_packet_size" validate:"default=65535"`             // 最大数据包大小，单位字节，超过会主动断开连接，默认65535
	MaxQueueBytes              int64  `toml:"max_queue_bytes" validate:"default=0"`                 // 最大消息队列的数量，超过会被丢弃，默认0-无最大值
	MaxQueueMessages           int32  `toml:"max_queue_messages" validate:"default=100"`            // 超过max_inflight_messages的消息会被缓存到队列中，默认100
	MessageSizeLimit           int32  `toml:"message_size_limit" validate:"default=0"`              // 允许发送的最大有效负载大小，默认0-无限制
	PersistentClientExpiration int64  `toml:"persistent_client_expiration" validate:"default=3600"` // 客户端断开超过该时间，broker会删除该会话信息，就收不到断开后的离线消息了，非标准选项，默认为0-永不
	QueueQos0Messages          bool   `toml:"queue_qos0_messages"`                                  // 设置为true，在持久化session的离线消息中，会有qos=0的消息也在排队，同时也会受max_queue_messages限制，默认false
	RetainAvailable            bool   `toml:"retain_available"`                                     // 设置为true表示，支持retain 消息，设置为false，不支持，当发送了保留位设置为1的消息，会断开连接
	SysInterval                int64  `toml:"sys_interval" validate:"default=10"`                   // 订阅$sys level topic树的更新时间，默认10s，设为0，表示禁用$sys level
	UpgradeOutgoingQos         bool   `toml:"upgrade_outgoing_qos"`                                 // 设置为true，改变qos等级，与订阅者的消息等级匹配，默认false
}
