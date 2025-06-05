// EXAMPLE: vecset_tutorial
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"
	"sort"

	"github.com/redis/go-redis/v9"
)

// HIDE_END

func ExampleClient_vectorset() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	defer rdb.Close()
	// REMOVE_START
	rdb.Del(ctx, "points", "quantSetQ8", "quantSetNoQ", "quantSetBin", "setNotReduced", "setReduced")
	// REMOVE_END

	// STEP_START vadd
	res1, err := rdb.VAdd(ctx, "points", "pt:A",
		&redis.VectorValues{Val: []float64{1.0, 1.0}},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res1) // >>> true

	res2, err := rdb.VAdd(ctx, "points", "pt:B",
		&redis.VectorValues{Val: []float64{-1.0, -1.0}},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res2) // >>> true

	res3, err := rdb.VAdd(ctx, "points", "pt:C",
		&redis.VectorValues{Val: []float64{-1.0, 1.0}},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res3) // >>> true

	res4, err := rdb.VAdd(ctx, "points", "pt:D",
		&redis.VectorValues{Val: []float64{1.0, -1.0}},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res4) // >>> true

	res5, err := rdb.VAdd(ctx, "points", "pt:E",
		&redis.VectorValues{Val: []float64{1.0, 0.0}},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res5) // >>> true

	res6, err := rdb.Type(ctx, "points").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res6) // >>> vectorset
	// STEP_END

	// STEP_START vcardvdim
	res7, err := rdb.VCard(ctx, "points").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res7) // >>> 5

	res8, err := rdb.VDim(ctx, "points").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res8) // >>> 2
	// STEP_END

	// STEP_START vemb
	res9, err := rdb.VEmb(ctx, "points", "pt:A", false).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res9) // >>> [0.9999999403953552 0.9999999403953552]

	res10, err := rdb.VEmb(ctx, "points", "pt:B", false).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res10) // >>> [-0.9999999403953552 -0.9999999403953552]

	res11, err := rdb.VEmb(ctx, "points", "pt:C", false).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res11) // >>> [-0.9999999403953552 0.9999999403953552]

	res12, err := rdb.VEmb(ctx, "points", "pt:D", false).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res12) // >>> [0.9999999403953552 -0.9999999403953552]

	res13, err := rdb.VEmb(ctx, "points", "pt:E", false).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res13) // >>> [1 0]
	// STEP_END

	// STEP_START attr
	attrs := map[string]interface{}{
		"name":        "Point A",
		"description": "First point added",
	}

	res14, err := rdb.VSetAttr(ctx, "points", "pt:A", attrs).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res14) // >>> true

	res15, err := rdb.VGetAttr(ctx, "points", "pt:A").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res15)
	// >>> {"description":"First point added","name":"Point A"}

	res16, err := rdb.VClearAttributes(ctx, "points", "pt:A").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res16) // >>> true

	// `VGetAttr()` returns an error if the attribute doesn't exist.
	_, err = rdb.VGetAttr(ctx, "points", "pt:A").Result()

	if err != nil {
		fmt.Println(err)
	}
	// STEP_END

	// STEP_START vrem
	res18, err := rdb.VAdd(ctx, "points", "pt:F",
		&redis.VectorValues{Val: []float64{0.0, 0.0}},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res18) // >>> true

	res19, err := rdb.VCard(ctx, "points").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res19) // >>> 6

	res20, err := rdb.VRem(ctx, "points", "pt:F").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res20) // >>> true

	res21, err := rdb.VCard(ctx, "points").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res21) // >>> 5
	// STEP_END

	// STEP_START vsim_basic
	res22, err := rdb.VSim(ctx, "points",
		&redis.VectorValues{Val: []float64{0.9, 0.1}},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res22) // >>> [pt:E pt:A pt:D pt:C pt:B]
	// STEP_END

	// STEP_START vsim_options
	res23, err := rdb.VSimWithArgsWithScores(
		ctx,
		"points",
		&redis.VectorRef{Name: "pt:A"},
		&redis.VSimArgs{Count: 4},
	).Result()

	if err != nil {
		panic(err)
	}

	sort.Slice(res23, func(i, j int) bool {
		return res23[i].Name < res23[j].Name
	})

	fmt.Println(res23)
	// >>> [{pt:A 1} {pt:C 0.5} {pt:D 0.5} {pt:E 0.8535534143447876}]
	// STEP_END

	// STEP_START vsim_filter
	// Set attributes for filtering
	res24, err := rdb.VSetAttr(ctx, "points", "pt:A",
		map[string]interface{}{
			"size":  "large",
			"price": 18.99,
		},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res24) // >>> true

	res25, err := rdb.VSetAttr(ctx, "points", "pt:B",
		map[string]interface{}{
			"size":  "large",
			"price": 35.99,
		},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res25) // >>> true

	res26, err := rdb.VSetAttr(ctx, "points", "pt:C",
		map[string]interface{}{
			"size":  "large",
			"price": 25.99,
		},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res26) // >>> true

	res27, err := rdb.VSetAttr(ctx, "points", "pt:D",
		map[string]interface{}{
			"size":  "small",
			"price": 21.00,
		},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res27) // >>> true

	res28, err := rdb.VSetAttr(ctx, "points", "pt:E",
		map[string]interface{}{
			"size":  "small",
			"price": 17.75,
		},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res28) // >>> true

	// Return elements in order of distance from point A whose
	// `size` attribute is `large`.
	res29, err := rdb.VSimWithArgs(ctx, "points",
		&redis.VectorRef{Name: "pt:A"},
		&redis.VSimArgs{Filter: `.size == "large"`},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res29) // >>> [pt:A pt:C pt:B]

	// Return elements in order of distance from point A whose size is
	// `large` and whose price is greater than 20.00.
	res30, err := rdb.VSimWithArgs(ctx, "points",
		&redis.VectorRef{Name: "pt:A"},
		&redis.VSimArgs{Filter: `.size == "large" && .price > 20.00`},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res30) // >>> [pt:C pt:B]
	// STEP_END

	// Output:
	// true
	// true
	// true
	// true
	// true
	// vectorset
	// 5
	// 2
	// [0.9999999403953552 0.9999999403953552]
	// [-0.9999999403953552 -0.9999999403953552]
	// [-0.9999999403953552 0.9999999403953552]
	// [0.9999999403953552 -0.9999999403953552]
	// [1 0]
	// true
	// {"description":"First point added","name":"Point A"}
	// true
	// redis: nil
	// true
	// 6
	// true
	// 5
	// [pt:E pt:A pt:D pt:C pt:B]
	// [{pt:A 1} {pt:C 0.5} {pt:D 0.5} {pt:E 0.8535534143447876}]
	// true
	// true
	// true
	// true
	// true
	// [pt:A pt:C pt:B]
	// [pt:C pt:B]
}

func ExampleClient_vectorset_quantization() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	defer rdb.Close()
	// REMOVE_START
	rdb.Del(ctx, "quantSetQ8", "quantSetNoQ", "quantSetBin")
	// REMOVE_END

	// STEP_START add_quant
	// Add with Q8 quantization
	vecQ := &redis.VectorValues{Val: []float64{1.262185, 1.958231}}

	res1, err := rdb.VAddWithArgs(ctx, "quantSetQ8", "quantElement", vecQ,
		&redis.VAddArgs{
			Q8: true,
		},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res1) // >>> true

	embQ8, err := rdb.VEmb(ctx, "quantSetQ8", "quantElement", false).Result()

	if err != nil {
		panic(err)
	}

	fmt.Printf("Q8 embedding: %v\n", embQ8)
	// >>> Q8 embedding: [1.2621850967407227 1.9582309722900391]

	// Add with NOQUANT option
	res2, err := rdb.VAddWithArgs(ctx, "quantSetNoQ", "quantElement", vecQ,
		&redis.VAddArgs{
			NoQuant: true,
		},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res2) // >>> true

	embNoQ, err := rdb.VEmb(ctx, "quantSetNoQ", "quantElement", false).Result()

	if err != nil {
		panic(err)
	}

	fmt.Printf("NOQUANT embedding: %v\n", embNoQ)
	// >>> NOQUANT embedding: [1.262185 1.958231]

	// Add with BIN quantization
	res3, err := rdb.VAddWithArgs(ctx, "quantSetBin", "quantElement", vecQ,
		&redis.VAddArgs{
			Bin: true,
		},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res3) // >>> true

	embBin, err := rdb.VEmb(ctx, "quantSetBin", "quantElement", false).Result()

	if err != nil {
		panic(err)
	}

	fmt.Printf("BIN embedding: %v\n", embBin)
	// >>> BIN embedding: [1 1]
	// STEP_END

	// Output:
	// true
	// Q8 embedding: [1.2643694877624512 1.958230972290039]
	// true
	// NOQUANT embedding: [1.262184977531433 1.958230972290039]
	// true
	// BIN embedding: [1 1]
}

func ExampleClient_vectorset_dimension_reduction() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	defer rdb.Close()
	// REMOVE_START
	rdb.Del(ctx, "setNotReduced", "setReduced")
	// REMOVE_END

	// STEP_START add_reduce
	// Create a vector with 300 dimensions
	values := make([]float64, 300)

	for i := 0; i < 300; i++ {
		values[i] = float64(i) / 299
	}

	vecLarge := &redis.VectorValues{Val: values}

	// Add without reduction
	res1, err := rdb.VAdd(ctx, "setNotReduced", "element", vecLarge).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res1) // >>> true

	dim1, err := rdb.VDim(ctx, "setNotReduced").Result()

	if err != nil {
		panic(err)
	}

	fmt.Printf("Dimension without reduction: %d\n", dim1)
	// >>> Dimension without reduction: 300

	// Add with reduction to 100 dimensions
	res2, err := rdb.VAddWithArgs(ctx, "setReduced", "element", vecLarge,
		&redis.VAddArgs{
			Reduce: 100,
		},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res2) // >>> true

	dim2, err := rdb.VDim(ctx, "setReduced").Result()

	if err != nil {
		panic(err)
	}

	fmt.Printf("Dimension after reduction: %d\n", dim2)
	// >>> Dimension after reduction: 100
	// STEP_END

	// Output:
	// true
	// Dimension without reduction: 300
	// true
	// Dimension after reduction: 100
}
