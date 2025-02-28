// EXAMPLE: geoindex
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// HIDE_END

func ExampleClient_geoindex() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
		Protocol: 2,
	})

	// REMOVE_START
	// make sure we are working with fresh database
	rdb.FlushDB(ctx)
	rdb.FTDropIndex(ctx, "productidx")
	rdb.FTDropIndex(ctx, "geomidx")
	rdb.Del(ctx, "product:46885", "product:46886", "shape:1", "shape:2", "shape:3", "shape:4")
	// REMOVE_END

	// STEP_START create_geo_idx
	geoCreateResult, err := rdb.FTCreate(ctx,
		"productidx",
		&redis.FTCreateOptions{
			OnJSON: true,
			Prefix: []interface{}{"product:"},
		},
		&redis.FieldSchema{
			FieldName: "$.location",
			As:        "location",
			FieldType: redis.SearchFieldTypeGeo,
		},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(geoCreateResult) // >>> OK
	// STEP_END

	// STEP_START add_geo_json
	prd46885 := map[string]interface{}{
		"description": "Navy Blue Slippers",
		"price":       45.99,
		"city":        "Denver",
		"location":    "-104.991531, 39.742043",
	}

	gjResult1, err := rdb.JSONSet(ctx, "product:46885", "$", prd46885).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(gjResult1) // >>> OK

	prd46886 := map[string]interface{}{
		"description": "Bright Green Socks",
		"price":       25.50,
		"city":        "Fort Collins",
		"location":    "-105.0618814,40.5150098",
	}

	gjResult2, err := rdb.JSONSet(ctx, "product:46886", "$", prd46886).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(gjResult2) // >>> OK
	// STEP_END

	// STEP_START geo_query
	geoQueryResult, err := rdb.FTSearch(ctx, "productidx",
		"@location:[-104.800644 38.846127 100 mi]",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(geoQueryResult)
	// >>> {1 [{product:46885...
	// STEP_END

	// STEP_START create_gshape_idx
	geomCreateResult, err := rdb.FTCreate(ctx, "geomidx",
		&redis.FTCreateOptions{
			OnJSON: true,
			Prefix: []interface{}{"shape:"},
		},
		&redis.FieldSchema{
			FieldName: "$.name",
			As:        "name",
			FieldType: redis.SearchFieldTypeText,
		},
		&redis.FieldSchema{
			FieldName:         "$.geom",
			As:                "geom",
			FieldType:         redis.SearchFieldTypeGeoShape,
			GeoShapeFieldType: "FLAT",
		},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(geomCreateResult) // >>> OK
	// STEP_END

	// STEP_START add_gshape_json
	shape1 := map[string]interface{}{
		"name": "Green Square",
		"geom": "POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))",
	}

	gmjResult1, err := rdb.JSONSet(ctx, "shape:1", "$", shape1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(gmjResult1) // >>> OK

	shape2 := map[string]interface{}{
		"name": "Red Rectangle",
		"geom": "POLYGON ((2 2.5, 2 3.5, 3.5 3.5, 3.5 2.5, 2 2.5))",
	}

	gmjResult2, err := rdb.JSONSet(ctx, "shape:2", "$", shape2).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(gmjResult2) // >>> OK

	shape3 := map[string]interface{}{
		"name": "Blue Triangle",
		"geom": "POLYGON ((3.5 1, 3.75 2, 4 1, 3.5 1))",
	}

	gmjResult3, err := rdb.JSONSet(ctx, "shape:3", "$", shape3).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(gmjResult3) // >>> OK

	shape4 := map[string]interface{}{
		"name": "Purple Point",
		"geom": "POINT (2 2)",
	}

	gmjResult4, err := rdb.JSONSet(ctx, "shape:4", "$", shape4).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(gmjResult4) // >>> OK
	// STEP_END

	// STEP_START gshape_query
	geomQueryResult, err := rdb.FTSearchWithArgs(ctx, "geomidx",
		"(-@name:(Green Square) @geom:[WITHIN $qshape])",
		&redis.FTSearchOptions{
			Params: map[string]interface{}{
				"qshape": "POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))",
			},
			DialectVersion: 4,
			Limit:          1,
		},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(geomQueryResult)
	// >>> {1 [{shape:4...
	// STEP_END

	// Output:
	// OK
	// OK
	// OK
	// {1 [{product:46885 <nil> <nil> <nil> map[$:{"city":"Denver","description":"Navy Blue Slippers","location":"-104.991531, 39.742043","price":45.99}]}]}
	// OK
	// OK
	// OK
	// OK
	// OK
	// {1 [{shape:4 <nil> <nil> <nil> map[$:[{"geom":"POINT (2 2)","name":"Purple Point"}]]}]}
}
