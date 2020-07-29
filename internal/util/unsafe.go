// +build !appengine

package util

import (
	"unsafe"
)

/**
通过unsafe包将byte切片和字符串互相转化，而且避免了产生新的内存申请。
这在需要大量处理byte切片转为字符串的应用场景中可以有很好的作用。
比如消息队列，大量减少小内存的申请减少GC的回收压力，对高并发的应用有一定程度的优化作用。
*/

//byte切片转字符串常规方式，会增加内存使用以及内存回收压力
//b := []byte{'a','b','c'}
//s := string(b) // 显示转换为字符串
// BytesToString converts byte slice to string.
func BytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

//string转byte切片常规方式
//cmdByte := []byte(cmdString)
// StringToBytes converts string to byte slice.
func StringToBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(
		&struct {
			string
			Cap int
		}{s, len(s)},
	))
}
