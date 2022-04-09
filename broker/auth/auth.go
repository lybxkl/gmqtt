package auth

type Manager interface {
	Authenticate(id string, cred interface{}) error
	//PlusVerify  增强版校验
	// param: authData 认证数据
	// return:
	//        d: 继续校验的认证数据
	//        continueAuth：true：成功，false：继续校验
	//        err != nil: 校验失败
	PlusVerify(authData []byte) (d []byte, continueAuth bool, err error) // 客户端自己验证时，忽略continueAuth
}

func NewDefaultAuth() Manager {
	return &auth{}
}

type auth struct {
	i int
}

func (a *auth) Authenticate(id string, cred interface{}) error {
	return nil
}

func (a *auth) PlusVerify(authData []byte) (d []byte, continueAuth bool, err error) {
	if a.i < 3 {
		a.i++
		return []byte("continue"), true, nil
	} else {
		a.i = 0
		return nil, false, nil
	}
}
