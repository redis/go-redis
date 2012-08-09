package redis

import (
	"bufio"
)

func readLine(rd *bufio.Reader) ([]byte, error) {
	line, isPrefix, err := rd.ReadLine()
	if err != nil {
		return line, err
	}
	if isPrefix {
		return line, ErrReaderTooSmall
	}
	return line, nil
}
