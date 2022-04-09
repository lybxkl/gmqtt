package authimpl

import (
	"gmqtt/broker/auth"
)

func NewDefaultAuth() auth.Manager {
	return &defaultAuth{}
}

type defaultAuth struct {
}

func (da *defaultAuth) Authenticate(id string, cred interface{}) error {
	return nil
}

func (da *defaultAuth) PlusVerify(authData []byte) (d []byte, continueAuth bool, err error) {
	return nil, false, nil
}
