package proto

// Author: Licoy
// Created: 2020/11/25
// Describe: struct serialization

type BinaryMarshaler interface {
	MarshalBinary() (data []byte, err error)
}

type BinaryUnmarshaler interface {
	UnmarshalBinary(data []byte) error
}

type RedisSerialization interface {
	BinaryMarshaler
	BinaryUnmarshaler
}
