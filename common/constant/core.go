package constant

const (
	// MWC is the multi-level wildcard
	MWC = "#"

	// SWC is the single level wildcard
	SWC = "+"

	// SEP is the topic level separator
	SEP = "/"

	// SYS is the starting character of the system level topics
	SYS = "$"

	// Both wildcards
	_WC = "#+"
)

const (
	// MaxQosAllowed is the maximum QOS supported by this server
	MaxQosAllowed byte = 0x02
)

const (
	StateCHR byte = iota // Regular character 普通字符
	StateMWC             // Multi-level wildcard 多层次的通配符
	StateSWC             // Single-level wildcard 单层通配符
	StateSEP             // Topic level separator 主题水平分隔符
	StateSYS             // System level topic ($) 系统级主题($)
)

const (
	AutoIdPrefix     = "auto-"
	KeepAlive        = 300
	MaxQueueMessages = 100
	SysInterval      = 10
	ConnectTimeout   = 5
	AckTimeout       = 20
	TimeoutRetries   = 3
)
