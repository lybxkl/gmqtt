package auth

import (
	"github.com/lybxkl/gmqtt/broker/core/auth"
	"github.com/lybxkl/gmqtt/broker/core/message"
	"github.com/lybxkl/gmqtt/broker/core/topic"
)

func NewDefaultAuth() auth.Manager {
	au := &defaultAuth{}
	au.RegistryPlus(&defaultPlusVerify{})
	return &manager{
		defaultAcl:  &defaultAcl{},
		defaultAuth: au,
	}
}

type manager struct {
	*defaultAcl
	*defaultAuth
}

type defaultAuth struct {
	i    int
	plus []auth.PlusVerify
}

func (a *defaultAuth) RegistryPlus(plus auth.PlusVerify) {
	a.plus = append(a.plus, plus)
}

func (a *defaultAuth) Verify(name string, cred interface{}) error {
	return nil
}

func (a *defaultAuth) PlusVerify(method string) (auth.PlusVerify, bool) {
	for _, plusVerify := range a.plus {
		if plusVerify.Should(method) {
			return plusVerify, true
		}
	}
	return nil, false
}

type defaultAcl struct {
}

func (a *defaultAcl) Sub(cid string, sub topic.Sub) bool {
	return true
}

func (a *defaultAcl) Pub(cid string, msg message.Message) bool {
	return true
}

type defaultPlusVerify struct {
}

func (d *defaultPlusVerify) Should(method string) bool {
	return true
}

func (d *defaultPlusVerify) Verify(authData []byte) (data []byte, continueAuth bool, err error) {
	return nil, false, nil
}
