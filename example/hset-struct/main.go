package main

import (
	"context"
	"time"

	"github.com/davecgh/go-spew/spew"

	"github.com/redis/go-redis/v9"
)

type Model struct {
	Str1    string     `redis:"str1"`
	Str2    string     `redis:"str2"`
	Str3    *string    `redis:"str3"`
	Str4    *string    `redis:"str4"`
	Bytes   []byte     `redis:"bytes"`
	Int     int        `redis:"int"`
	Int2    *int       `redis:"int2"`
	Int3    *int       `redis:"int3"`
	Bool    bool       `redis:"bool"`
	Bool2   *bool      `redis:"bool2"`
	Bool3   *bool      `redis:"bool3"`
	Bool4   *bool      `redis:"bool4,omitempty"`
	Time    time.Time  `redis:"time"`
	Time2   *time.Time `redis:"time2"`
	Time3   *time.Time `redis:"time3"`
	Ignored struct{}   `redis:"-"`
}

func main() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr: ":6379",
	})

	_ = rdb.FlushDB(ctx).Err()

	t := time.Date(2025, 02, 8, 0, 0, 0, 0, time.UTC)

	data := Model{
		Str1:    "hello",
		Str2:    "world",
		Str3:    ToPtr("hello"),
		Str4:    nil,
		Bytes:   []byte("this is bytes !"),
		Int:     123,
		Int2:    ToPtr(0),
		Int3:    nil,
		Bool:    true,
		Bool2:   ToPtr(false),
		Bool3:   nil,
		Time:    t,
		Time2:   ToPtr(t),
		Time3:   nil,
		Ignored: struct{}{},
	}

	// Set some fields.
	if _, err := rdb.Pipelined(ctx, func(rdb redis.Pipeliner) error {
		rdb.HMSet(ctx, "key", data)
		return nil
	}); err != nil {
		panic(err)
	}

	var model1, model2 Model

	// Scan all fields into the model.
	if err := rdb.HGetAll(ctx, "key").Scan(&model1); err != nil {
		panic(err)
	}

	// Or scan a subset of the fields.
	if err := rdb.HMGet(ctx, "key", "str1", "int").Scan(&model2); err != nil {
		panic(err)
	}

	spew.Dump(model1)
	// Output:
	// (main.Model) {
	//  Str1: (string) (len=5) "hello",
	//  Str2: (string) (len=5) "world",
	//  Str3: (*string)(0xc000016970)((len=5) "hello"),
	//  Str4: (*string)(0xc000016980)(""),
	//  Bytes: ([]uint8) (len=15 cap=16) {
	//   00000000  74 68 69 73 20 69 73 20  62 79 74 65 73 20 21     |this is bytes !|
	//  },
	//  Int: (int) 123,
	//  Int2: (*int)(0xc000014568)(0),
	//  Int3: (*int)(0xc000014560)(0),
	//  Bool: (bool) true,
	//  Bool2: (*bool)(0xc000014570)(false),
	//  Bool3: (*bool)(0xc000014548)(false),
	//  Bool4: (*bool)(<nil>),
	//  Time: (time.Time) 2025-02-08 00:00:00 +0000 UTC,
	//  Time2: (*time.Time)(0xc0000122a0)(2025-02-08 00:00:00 +0000 UTC),
	//  Time3: (*time.Time)(0xc000012288)(0001-01-01 00:00:00 +0000 UTC),
	//  Ignored: (struct {}) {
	//  }
	// }

	spew.Dump(model2)
	// Output:
	// (main.Model) {
	//  Str1: (string) (len=5) "hello",
	//  Str2: (string) "",
	//  Str3: (*string)(<nil>),
	//  Str4: (*string)(<nil>),
	//  Bytes: ([]uint8) <nil>,
	//  Int: (int) 123,
	//  Int2: (*int)(<nil>),
	//  Int3: (*int)(<nil>),
	//  Bool: (bool) false,
	//  Bool2: (*bool)(<nil>),
	//  Bool3: (*bool)(<nil>),
	//  Bool4: (*bool)(<nil>),
	//  Time: (time.Time) 0001-01-01 00:00:00 +0000 UTC,
	//  Time2: (*time.Time)(<nil>),
	//  Time3: (*time.Time)(<nil>),
	//  Ignored: (struct {}) {
	//  }
	// }
}

func ToPtr[T any](v T) *T {
	return &v
}
