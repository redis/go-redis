package main

func setArgs2(dst []interface{}, arg0, arg1 interface{}) []interface{} {
	if cap(dst) >= 2 {
		dst = dst[:2]
	} else {
		dst = make([]interface{}, 2)
	}
	dst[0] = arg0
	dst[1] = arg1
	return dst
}

func testFunc(key string) []interface{} {
	var args []interface{}
	args = setArgs2(args, "get", key)
	return args
}

func main() {
	_ = testFunc("mykey")
}

