// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package assert

func Must(b bool) {
	if b {
		return
	}
	//log.Panic("assertion failed")
}

func MustNoError(err error) {
	if err == nil {
		return
	}
	//log.PanicError(err, "error happens, assertion failed")
}
