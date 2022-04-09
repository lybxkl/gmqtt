package runtimex

import (
	. "gmqtt/common/log"
)

func Recover() {
	if err := recover(); err != nil {
		Log.Errorf("recover: %+v", err)
	}
}
