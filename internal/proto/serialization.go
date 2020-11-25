package proto

// Author: Licoy
// Created: 2020/11/25
// Describe: struct serialization

type RedisBinaryMarshaler interface {
	RedisMarshalBinary() (data []byte, err error)
}

type RedisBinaryUnmarshaler interface {
	RedisUnmarshalBinary(data []byte) error
}
