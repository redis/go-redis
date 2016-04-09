package internal

import (
	"fmt"
	"io/ioutil"
	"log"
)

var Debug bool

var Logger = log.New(ioutil.Discard, "redis: ", log.LstdFlags)

func Debugf(s string, args ...interface{}) {
	if !Debug {
		return
	}
	Logger.Output(2, fmt.Sprintf(s, args...))
}

func Logf(s string, args ...interface{}) {
	Logger.Output(2, fmt.Sprintf(s, args...))
}
