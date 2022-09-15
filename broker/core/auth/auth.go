package auth

import (
	"errors"

	"github.com/lybxkl/gmqtt/broker/core/message"
	"github.com/lybxkl/gmqtt/broker/core/topic"
)

var (
	ErrNotAllowAuthMethod = errors.New("not allow auth method")
	ErrAuthFail           = errors.New("auth fail")
)

type Manager interface {
	Auth
	Acl
}

type Auth interface {
	// Verify 普通校验
	Verify(name string, cred interface{}) error
	//PlusVerify  增强版校验

	PlusVerify(method string) (PlusVerify, bool)
}

type PlusVerify interface {
	Should(method string) bool
	// Verify
	// param: authData 认证数据
	// return:
	//        d: 继续校验的认证数据
	//        continueAuth：true：成功，false：继续校验
	//        err != nil: 校验失败
	Verify(authData []byte) (d []byte, continueAuth bool, err error) // 客户端自己验证时，忽略continueAuth
}

type Acl interface {
	Sub(cid string, sub topic.Sub) bool
	Pub(cid string, msg message.Message) bool
}
