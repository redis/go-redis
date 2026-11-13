package redis

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

var nullableBenchmarkSink uint64

func nullableBenchmarkValue(row, col int) (string, bool) {
	if (row*105+col)%17 == 0 {
		return "", false
	}
	if col%11 == 0 {
		return "", true
	}
	if col == 1 {
		return strconv.FormatInt(9007199254740993+int64(row), 10), true
	}
	if col == 2 {
		return "x\x00雪\r\n", true
	}
	return "r" + strconv.Itoa(row) + "c" + strconv.Itoa(col), true
}

func BenchmarkHMGetStringsDecode(b *testing.B) {
	for _, count := range []int{256, 1024, 4096} {
		var wire bytes.Buffer
		for row := 0; row < count; row++ {
			fmt.Fprint(&wire, "*105\r\n")
			for col := range 105 {
				s, valid := nullableBenchmarkValue(row, col)
				if !valid {
					fmt.Fprint(&wire, "_\r\n")
				} else {
					fmt.Fprintf(&wire, "$%d\r\n%s\r\n", len(s), s)
				}
			}
		}
		payload := wire.Bytes()
		args := make([]interface{}, 107)
		args[0], args[1] = "hmget", "key"
		for col := range 105 {
			args[2+col] = "f" + strconv.Itoa(col)
		}
		for _, size := range []int{32 << 10, 1 << 20} {
			for _, mode := range []string{"generic", "nullable"} {
				b.Run(fmt.Sprintf("rows_%d/buffer_%d/%s", count, size, mode), func(b *testing.B) {
					generic := make([]*SliceCmd, count)
					typed := make([]*NullableStringSliceCmd, count)
					for row := range count {
						a := append([]interface{}(nil), args...)
						if mode == "generic" {
							generic[row] = NewSliceCmd(context.Background(), a...)
						} else {
							typed[row] = NewNullableStringSliceCmd(context.Background(), a...)
						}
					}
					reader := bytes.NewReader(payload)
					rd := proto.NewReaderSize(reader, size)
					decode := func() error {
						reader.Reset(payload)
						rd.Reset(reader)
						for row := range count {
							var err error
							if mode == "generic" {
								err = generic[row].readReply(rd)
							} else {
								err = typed[row].readReply(rd)
							}
							if err != nil {
								return err
							}
						}
						return nil
					}
					consume := func() uint64 {
						var sum uint64
						for row := range count {
							for col := range 105 {
								var s string
								var valid bool
								if mode == "generic" {
									s, valid = generic[row].val[col].(string)
								} else {
									s, valid = typed[row].val.Values[col], typed[row].val.Valid[col]
								}
								if valid {
									sum += 1 + uint64(len(s))
									if len(s) > 0 {
										sum += uint64(s[0])
									}
								}
							}
						}
						return sum
					}
					validate := func() {
						for row := range count {
							for col := range 105 {
								want, valid := nullableBenchmarkValue(row, col)
								if mode == "generic" {
									got, ok := generic[row].val[col].(string)
									if got != want || ok != valid {
										b.Fatal("generic byte/null mismatch")
									}
								} else {
									got := typed[row].val
									if got.Values[col] != want || got.Valid[col] != valid {
										b.Fatal("nullable byte/null mismatch")
									}
								}
							}
						}
					}
					if err := decode(); err != nil {
						b.Fatal(err)
					}
					validate()
					want := consume()
					b.ReportAllocs()
					b.SetBytes(int64(len(payload)))
					b.ResetTimer()
					for range b.N {
						if err := decode(); err != nil {
							b.Fatal(err)
						}
						nullableBenchmarkSink = consume()
					}
					b.StopTimer()
					if nullableBenchmarkSink != want {
						b.Fatal("consumption checksum changed")
					}
					validate()
				})
			}
		}
	}
}
