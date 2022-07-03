package util

import (
	"bytes"

	"github.com/lybxkl/gmqtt/common/constant"
)

var (
	sharePrefix    = []byte("$share/")
	sharePrefixLen = len(sharePrefix)

	sysPrefix    = []byte("$sys/")
	sysPrefixLen = len(sysPrefix)
)

// ShareTopicPrefixLen 共享订阅主题前缀长度
func ShareTopicPrefixLen() int {
	return sharePrefixLen
}

// SysTopicPrefixLen 系统订阅主题前缀长度
func SysTopicPrefixLen() int {
	return sysPrefixLen
}

// IsShareSub 判断是否共享订阅前缀
func IsShareSub(topic []byte) bool {
	return bytes.HasPrefix(topic, sharePrefix)
}

// IsSysSub 判断是否系统主题前缀
func IsSysSub(topic []byte) bool {
	return bytes.HasPrefix(topic, sysPrefix)
}

// Qos 计算服务支持最大qos
func Qos(qos byte) byte {
	if qos > constant.MaxQosAllowed {
		qos = constant.MaxQosAllowed
	}
	return qos
}
