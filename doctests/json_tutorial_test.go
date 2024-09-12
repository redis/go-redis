// EXAMPLE: json_tutorial
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// HIDE_END
func ExampleClient_setget() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bike")
	// REMOVE_END

	// STEP_START set_get
	res1, err := rdb.JSONSet(ctx, "bike", "$",
		"\"Hyperion\"",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res1) // >>> OK

	res2, err := rdb.JSONGet(ctx, "bike", "$").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res2) // >>> ["Hyperion"]

	res3, err := rdb.JSONType(ctx, "bike", "$").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res3) // >>> [[string]]
	// STEP_END

	// Output:
	// OK
	// ["Hyperion"]
	// [[string]]
}

func ExampleClient_str() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bike")
	// REMOVE_END

	_, err := rdb.JSONSet(ctx, "bike", "$",
		"\"Hyperion\"",
	).Result()

	if err != nil {
		panic(err)
	}

	// STEP_START str
	res4, err := rdb.JSONStrLen(ctx, "bike", "$").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(*res4[0]) // >>> 8

	res5, err := rdb.JSONStrAppend(ctx, "bike", "$", "\" (Enduro bikes)\"").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(*res5[0]) // >>> 23

	res6, err := rdb.JSONGet(ctx, "bike", "$").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res6) // >>> ["Hyperion (Enduro bikes)"]
	// STEP_END

	// Output:
	// 8
	// 23
	// ["Hyperion (Enduro bikes)"]
}

func ExampleClient_num() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "crashes")
	// REMOVE_END

	// STEP_START num
	res7, err := rdb.JSONSet(ctx, "crashes", "$", 0).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res7) // >>> OK

	res8, err := rdb.JSONNumIncrBy(ctx, "crashes", "$", 1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res8) // >>> [1]

	res9, err := rdb.JSONNumIncrBy(ctx, "crashes", "$", 1.5).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res9) // >>> [2.5]

	res10, err := rdb.JSONNumIncrBy(ctx, "crashes", "$", -0.75).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res10) // >>> [1.75]
	// STEP_END

	// Output:
	// OK
	// [1]
	// [2.5]
	// [1.75]
}

func ExampleClient_arr() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "newbike")
	// REMOVE_END

	// STEP_START arr
	res11, err := rdb.JSONSet(ctx, "newbike", "$",
		[]interface{}{
			"Deimos",
			map[string]interface{}{"crashes": 0},
			nil,
		},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res11) // >>> OK

	res12, err := rdb.JSONGet(ctx, "newbike", "$").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res12) // >>> [["Deimos",{"crashes":0},null]]

	res13, err := rdb.JSONGet(ctx, "newbike", "$[1].crashes").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res13) // >>> [0]

	res14, err := rdb.JSONDel(ctx, "newbike", "$.[-1]").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res14) // >>> 1

	res15, err := rdb.JSONGet(ctx, "newbike", "$").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res15) // >>> [["Deimos",{"crashes":0}]]
	// STEP_END

	// Output:
	// OK
	// [["Deimos",{"crashes":0},null]]
	// [0]
	// 1
	// [["Deimos",{"crashes":0}]]
}

func ExampleClient_arr2() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "riders")
	// REMOVE_END

	// STEP_START arr2
	res16, err := rdb.JSONSet(ctx, "riders", "$", []interface{}{}).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res16) // >>> OK

	res17, err := rdb.JSONArrAppend(ctx, "riders", "$", "\"Norem\"").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res17) // >>> [1]

	res18, err := rdb.JSONGet(ctx, "riders", "$").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res18) // >>> [["Norem"]]

	res19, err := rdb.JSONArrInsert(ctx, "riders", "$", 1,
		"\"Prickett\"", "\"Royce\"", "\"Castilla\"",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res19) // [3]

	res20, err := rdb.JSONGet(ctx, "riders", "$").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res20) // >>> [["Norem", "Prickett", "Royce", "Castilla"]]

	rangeStop := 1

	res21, err := rdb.JSONArrTrimWithArgs(ctx, "riders", "$",
		&redis.JSONArrTrimArgs{Start: 1, Stop: &rangeStop},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res21) // >>> [1]

	res22, err := rdb.JSONGet(ctx, "riders", "$").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res22) // >>> [["Prickett"]]

	res23, err := rdb.JSONArrPop(ctx, "riders", "$", -1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res23) // >>> [["Prickett"]]

	res24, err := rdb.JSONArrPop(ctx, "riders", "$", -1).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res24) // []
	// STEP_END

	// Output:
	// OK
	// [1]
	// [["Norem"]]
	// [4]
	// [["Norem","Prickett","Royce","Castilla"]]
	// [1]
	// [["Prickett"]]
	// ["Prickett"]
	// []
}

func ExampleClient_obj() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bike:1")
	// REMOVE_END

	// STEP_START obj
	res25, err := rdb.JSONSet(ctx, "bike:1", "$",
		map[string]interface{}{
			"model": "Deimos",
			"brand": "Ergonom",
			"price": 4972,
		},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res25) // >>> OK

	res26, err := rdb.JSONObjLen(ctx, "bike:1", "$").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(*res26[0]) // >>> 3

	res27, err := rdb.JSONObjKeys(ctx, "bike:1", "$").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res27) // >>> [brand model price]
	// STEP_END

	// Output:
	// OK
	// 3
	// [[brand model price]]
}

var inventory_json = map[string]interface{}{
	"inventory": map[string]interface{}{
		"mountain_bikes": []interface{}{
			map[string]interface{}{
				"id":    "bike:1",
				"model": "Phoebe",
				"description": "This is a mid-travel trail slayer that is a fantastic " +
					"daily driver or one bike quiver. The Shimano Claris 8-speed groupset " +
					"gives plenty of gear range to tackle hills and there\u2019s room for " +
					"mudguards and a rack too.  This is the bike for the rider who wants " +
					"trail manners with low fuss ownership.",
				"price":  1920,
				"specs":  map[string]interface{}{"material": "carbon", "weight": 13.1},
				"colors": []interface{}{"black", "silver"},
			},
			map[string]interface{}{
				"id":    "bike:2",
				"model": "Quaoar",
				"description": "Redesigned for the 2020 model year, this bike " +
					"impressed our testers and is the best all-around trail bike we've " +
					"ever tested. The Shimano gear system effectively does away with an " +
					"external cassette, so is super low maintenance in terms of wear " +
					"and tear. All in all it's an impressive package for the price, " +
					"making it very competitive.",
				"price":  2072,
				"specs":  map[string]interface{}{"material": "aluminium", "weight": 7.9},
				"colors": []interface{}{"black", "white"},
			},
			map[string]interface{}{
				"id":    "bike:3",
				"model": "Weywot",
				"description": "This bike gives kids aged six years and older " +
					"a durable and uberlight mountain bike for their first experience " +
					"on tracks and easy cruising through forests and fields. A set of " +
					"powerful Shimano hydraulic disc brakes provide ample stopping " +
					"ability. If you're after a budget option, this is one of the best " +
					"bikes you could get.",
				"price": 3264,
				"specs": map[string]interface{}{"material": "alloy", "weight": 13.8},
			},
		},
		"commuter_bikes": []interface{}{
			map[string]interface{}{
				"id":    "bike:4",
				"model": "Salacia",
				"description": "This bike is a great option for anyone who just " +
					"wants a bike to get about on With a slick-shifting Claris gears " +
					"from Shimano\u2019s, this is a bike which doesn\u2019t break the " +
					"bank and delivers craved performance.  It\u2019s for the rider " +
					"who wants both efficiency and capability.",
				"price":  1475,
				"specs":  map[string]interface{}{"material": "aluminium", "weight": 16.6},
				"colors": []interface{}{"black", "silver"},
			},
			map[string]interface{}{
				"id":    "bike:5",
				"model": "Mimas",
				"description": "A real joy to ride, this bike got very high " +
					"scores in last years Bike of the year report. The carefully " +
					"crafted 50-34 tooth chainset and 11-32 tooth cassette give an " +
					"easy-on-the-legs bottom gear for climbing, and the high-quality " +
					"Vittoria Zaffiro tires give balance and grip.It includes " +
					"a low-step frame , our memory foam seat, bump-resistant shocks and " +
					"conveniently placed thumb throttle. Put it all together and you " +
					"get a bike that helps redefine what can be done for this price.",
				"price": 3941,
				"specs": map[string]interface{}{"material": "alloy", "weight": 11.6},
			},
		},
	},
}

func ExampleClient_setbikes() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bikes:inventory")
	// REMOVE_END

	// STEP_START set_bikes
	var inventory_json = map[string]interface{}{
		"inventory": map[string]interface{}{
			"mountain_bikes": []interface{}{
				map[string]interface{}{
					"id":    "bike:1",
					"model": "Phoebe",
					"description": "This is a mid-travel trail slayer that is a fantastic " +
						"daily driver or one bike quiver. The Shimano Claris 8-speed groupset " +
						"gives plenty of gear range to tackle hills and there\u2019s room for " +
						"mudguards and a rack too.  This is the bike for the rider who wants " +
						"trail manners with low fuss ownership.",
					"price":  1920,
					"specs":  map[string]interface{}{"material": "carbon", "weight": 13.1},
					"colors": []interface{}{"black", "silver"},
				},
				map[string]interface{}{
					"id":    "bike:2",
					"model": "Quaoar",
					"description": "Redesigned for the 2020 model year, this bike " +
						"impressed our testers and is the best all-around trail bike we've " +
						"ever tested. The Shimano gear system effectively does away with an " +
						"external cassette, so is super low maintenance in terms of wear " +
						"and tear. All in all it's an impressive package for the price, " +
						"making it very competitive.",
					"price":  2072,
					"specs":  map[string]interface{}{"material": "aluminium", "weight": 7.9},
					"colors": []interface{}{"black", "white"},
				},
				map[string]interface{}{
					"id":    "bike:3",
					"model": "Weywot",
					"description": "This bike gives kids aged six years and older " +
						"a durable and uberlight mountain bike for their first experience " +
						"on tracks and easy cruising through forests and fields. A set of " +
						"powerful Shimano hydraulic disc brakes provide ample stopping " +
						"ability. If you're after a budget option, this is one of the best " +
						"bikes you could get.",
					"price": 3264,
					"specs": map[string]interface{}{"material": "alloy", "weight": 13.8},
				},
			},
			"commuter_bikes": []interface{}{
				map[string]interface{}{
					"id":    "bike:4",
					"model": "Salacia",
					"description": "This bike is a great option for anyone who just " +
						"wants a bike to get about on With a slick-shifting Claris gears " +
						"from Shimano\u2019s, this is a bike which doesn\u2019t break the " +
						"bank and delivers craved performance.  It\u2019s for the rider " +
						"who wants both efficiency and capability.",
					"price":  1475,
					"specs":  map[string]interface{}{"material": "aluminium", "weight": 16.6},
					"colors": []interface{}{"black", "silver"},
				},
				map[string]interface{}{
					"id":    "bike:5",
					"model": "Mimas",
					"description": "A real joy to ride, this bike got very high " +
						"scores in last years Bike of the year report. The carefully " +
						"crafted 50-34 tooth chainset and 11-32 tooth cassette give an " +
						"easy-on-the-legs bottom gear for climbing, and the high-quality " +
						"Vittoria Zaffiro tires give balance and grip.It includes " +
						"a low-step frame , our memory foam seat, bump-resistant shocks and " +
						"conveniently placed thumb throttle. Put it all together and you " +
						"get a bike that helps redefine what can be done for this price.",
					"price": 3941,
					"specs": map[string]interface{}{"material": "alloy", "weight": 11.6},
				},
			},
		},
	}

	res1, err := rdb.JSONSet(ctx, "bikes:inventory", "$", inventory_json).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res1) // >>> OK
	// STEP_END

	// Output:
	// OK
}

func ExampleClient_getbikes() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bikes:inventory")
	// REMOVE_END

	_, err := rdb.JSONSet(ctx, "bikes:inventory", "$", inventory_json).Result()

	if err != nil {
		panic(err)
	}

	// STEP_START get_bikes
	res2, err := rdb.JSONGetWithArgs(ctx, "bikes:inventory",
		&redis.JSONGetArgs{Indent: "  ", Newline: "\n", Space: " "},
		"$.inventory.*",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res2)
	// >>>
	// [
	//   [
	//     {
	//       "colors": [
	//         "black",
	//         "silver"
	// ...
	// STEP_END

	// Output:
	// [
	//   [
	//     {
	//       "colors": [
	//         "black",
	//         "silver"
	//       ],
	//       "description": "This bike is a great option for anyone who just wants a bike to get about on With a slick-shifting Claris gears from Shimano’s, this is a bike which doesn’t break the bank and delivers craved performance.  It’s for the rider who wants both efficiency and capability.",
	//       "id": "bike:4",
	//       "model": "Salacia",
	//       "price": 1475,
	//       "specs": {
	//         "material": "aluminium",
	//         "weight": 16.6
	//       }
	//     },
	//     {
	//       "description": "A real joy to ride, this bike got very high scores in last years Bike of the year report. The carefully crafted 50-34 tooth chainset and 11-32 tooth cassette give an easy-on-the-legs bottom gear for climbing, and the high-quality Vittoria Zaffiro tires give balance and grip.It includes a low-step frame , our memory foam seat, bump-resistant shocks and conveniently placed thumb throttle. Put it all together and you get a bike that helps redefine what can be done for this price.",
	//       "id": "bike:5",
	//       "model": "Mimas",
	//       "price": 3941,
	//       "specs": {
	//         "material": "alloy",
	//         "weight": 11.6
	//       }
	//     }
	//   ],
	//   [
	//     {
	//       "colors": [
	//         "black",
	//         "silver"
	//       ],
	//       "description": "This is a mid-travel trail slayer that is a fantastic daily driver or one bike quiver. The Shimano Claris 8-speed groupset gives plenty of gear range to tackle hills and there’s room for mudguards and a rack too.  This is the bike for the rider who wants trail manners with low fuss ownership.",
	//       "id": "bike:1",
	//       "model": "Phoebe",
	//       "price": 1920,
	//       "specs": {
	//         "material": "carbon",
	//         "weight": 13.1
	//       }
	//     },
	//     {
	//       "colors": [
	//         "black",
	//         "white"
	//       ],
	//       "description": "Redesigned for the 2020 model year, this bike impressed our testers and is the best all-around trail bike we've ever tested. The Shimano gear system effectively does away with an external cassette, so is super low maintenance in terms of wear and tear. All in all it's an impressive package for the price, making it very competitive.",
	//       "id": "bike:2",
	//       "model": "Quaoar",
	//       "price": 2072,
	//       "specs": {
	//         "material": "aluminium",
	//         "weight": 7.9
	//       }
	//     },
	//     {
	//       "description": "This bike gives kids aged six years and older a durable and uberlight mountain bike for their first experience on tracks and easy cruising through forests and fields. A set of powerful Shimano hydraulic disc brakes provide ample stopping ability. If you're after a budget option, this is one of the best bikes you could get.",
	//       "id": "bike:3",
	//       "model": "Weywot",
	//       "price": 3264,
	//       "specs": {
	//         "material": "alloy",
	//         "weight": 13.8
	//       }
	//     }
	//   ]
	// ]
}

func ExampleClient_getmtnbikes() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bikes:inventory")
	// REMOVE_END

	_, err := rdb.JSONSet(ctx, "bikes:inventory", "$", inventory_json).Result()

	if err != nil {
		panic(err)
	}

	// STEP_START get_mtnbikes
	res3, err := rdb.JSONGet(ctx, "bikes:inventory",
		"$.inventory.mountain_bikes[*].model",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res3)
	// >>> ["Phoebe","Quaoar","Weywot"]

	res4, err := rdb.JSONGet(ctx,
		"bikes:inventory", "$.inventory[\"mountain_bikes\"][*].model",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res4)
	// >>> ["Phoebe","Quaoar","Weywot"]

	res5, err := rdb.JSONGet(ctx,
		"bikes:inventory", "$..mountain_bikes[*].model",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res5)
	// >>> ["Phoebe","Quaoar","Weywot"]
	// STEP_END

	// Output:
	// ["Phoebe","Quaoar","Weywot"]
	// ["Phoebe","Quaoar","Weywot"]
	// ["Phoebe","Quaoar","Weywot"]
}

func ExampleClient_getmodels() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bikes:inventory")
	// REMOVE_END

	_, err := rdb.JSONSet(ctx, "bikes:inventory", "$", inventory_json).Result()

	if err != nil {
		panic(err)
	}

	// STEP_START get_models
	res6, err := rdb.JSONGet(ctx, "bikes:inventory", "$..model").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res6) // >>> ["Salacia","Mimas","Phoebe","Quaoar","Weywot"]
	// STEP_END

	// Output:
	// ["Salacia","Mimas","Phoebe","Quaoar","Weywot"]
}

func ExampleClient_get2mtnbikes() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bikes:inventory")
	// REMOVE_END

	_, err := rdb.JSONSet(ctx, "bikes:inventory", "$", inventory_json).Result()

	if err != nil {
		panic(err)
	}

	// STEP_START get2mtnbikes
	res7, err := rdb.JSONGet(ctx, "bikes:inventory", "$..mountain_bikes[0:2].model").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res7) // >>> ["Phoebe","Quaoar"]
	// STEP_END

	// Output:
	// ["Phoebe","Quaoar"]
}

func ExampleClient_filter1() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bikes:inventory")
	// REMOVE_END

	_, err := rdb.JSONSet(ctx, "bikes:inventory", "$", inventory_json).Result()

	if err != nil {
		panic(err)
	}

	// STEP_START filter1
	res8, err := rdb.JSONGetWithArgs(ctx, "bikes:inventory",
		&redis.JSONGetArgs{Indent: "  ", Newline: "\n", Space: " "},
		"$..mountain_bikes[?(@.price < 3000 && @.specs.weight < 10)]",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res8)
	// >>>
	// [
	//   {
	//     "colors": [
	//       "black",
	//       "white"
	//     ],
	//     "description": "Redesigned for the 2020 model year
	// ...
	// STEP_END

	// Output:
	// [
	//   {
	//     "colors": [
	//       "black",
	//       "white"
	//     ],
	//     "description": "Redesigned for the 2020 model year, this bike impressed our testers and is the best all-around trail bike we've ever tested. The Shimano gear system effectively does away with an external cassette, so is super low maintenance in terms of wear and tear. All in all it's an impressive package for the price, making it very competitive.",
	//     "id": "bike:2",
	//     "model": "Quaoar",
	//     "price": 2072,
	//     "specs": {
	//       "material": "aluminium",
	//       "weight": 7.9
	//     }
	//   }
	// ]
}

func ExampleClient_filter2() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bikes:inventory")
	// REMOVE_END

	_, err := rdb.JSONSet(ctx, "bikes:inventory", "$", inventory_json).Result()

	if err != nil {
		panic(err)
	}

	// STEP_START filter2
	res9, err := rdb.JSONGet(ctx,
		"bikes:inventory",
		"$..[?(@.specs.material == 'alloy')].model",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res9) // >>> ["Mimas","Weywot"]
	// STEP_END

	// Output:
	// ["Mimas","Weywot"]
}

func ExampleClient_filter3() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bikes:inventory")
	// REMOVE_END

	_, err := rdb.JSONSet(ctx, "bikes:inventory", "$", inventory_json).Result()

	if err != nil {
		panic(err)
	}

	// STEP_START filter3
	res10, err := rdb.JSONGet(ctx,
		"bikes:inventory",
		"$..[?(@.specs.material =~ '(?i)al')].model",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res10) // >>> ["Salacia","Mimas","Quaoar","Weywot"]
	// STEP_END

	// Output:
	// ["Salacia","Mimas","Quaoar","Weywot"]
}

func ExampleClient_filter4() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bikes:inventory")
	// REMOVE_END

	_, err := rdb.JSONSet(ctx, "bikes:inventory", "$", inventory_json).Result()

	if err != nil {
		panic(err)
	}

	// STEP_START filter4
	res11, err := rdb.JSONSet(ctx,
		"bikes:inventory",
		"$.inventory.mountain_bikes[0].regex_pat",
		"\"(?i)al\"",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res11) // >>> OK

	res12, err := rdb.JSONSet(ctx,
		"bikes:inventory",
		"$.inventory.mountain_bikes[1].regex_pat",
		"\"(?i)al\"",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res12) // >>> OK

	res13, err := rdb.JSONSet(ctx,
		"bikes:inventory",
		"$.inventory.mountain_bikes[2].regex_pat",
		"\"(?i)al\"",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res13) // >>> OK

	res14, err := rdb.JSONGet(ctx,
		"bikes:inventory",
		"$.inventory.mountain_bikes[?(@.specs.material =~ @.regex_pat)].model",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res14) // >>> ["Quaoar","Weywot"]
	// STEP_END

	// Output:
	// OK
	// OK
	// OK
	// ["Quaoar","Weywot"]
}

func ExampleClient_updatebikes() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bikes:inventory")
	// REMOVE_END

	_, err := rdb.JSONSet(ctx, "bikes:inventory", "$", inventory_json).Result()

	if err != nil {
		panic(err)
	}

	// STEP_START update_bikes
	res15, err := rdb.JSONGet(ctx, "bikes:inventory", "$..price").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res15) // >>> [1475,3941,1920,2072,3264]

	res16, err := rdb.JSONNumIncrBy(ctx, "bikes:inventory", "$..price", -100).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res16) // >>> [1375,3841,1820,1972,3164]

	res17, err := rdb.JSONNumIncrBy(ctx, "bikes:inventory", "$..price", 100).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res17) // >>> [1475,3941,1920,2072,3264]
	// STEP_END

	// Output:
	// [1475,3941,1920,2072,3264]
	// [1375,3841,1820,1972,3164]
	// [1475,3941,1920,2072,3264]
}

func ExampleClient_updatefilters1() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bikes:inventory")
	// REMOVE_END

	_, err := rdb.JSONSet(ctx, "bikes:inventory", "$", inventory_json).Result()

	if err != nil {
		panic(err)
	}

	// STEP_START update_filters1
	res18, err := rdb.JSONSet(ctx,
		"bikes:inventory",
		"$.inventory.*[?(@.price<2000)].price",
		1500,
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res18) // >>> OK

	res19, err := rdb.JSONGet(ctx, "bikes:inventory", "$..price").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res19) // >>> [1500,3941,1500,2072,3264]
	// STEP_END

	// Output:
	// OK
	// [1500,3941,1500,2072,3264]
}

func ExampleClient_updatefilters2() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// REMOVE_START
	rdb.Del(ctx, "bikes:inventory")
	// REMOVE_END

	_, err := rdb.JSONSet(ctx, "bikes:inventory", "$", inventory_json).Result()

	if err != nil {
		panic(err)
	}

	// STEP_START update_filters2
	res20, err := rdb.JSONArrAppend(ctx,
		"bikes:inventory",
		"$.inventory.*[?(@.price<2000)].colors",
		"\"pink\"",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res20) // >>> [3 3]

	res21, err := rdb.JSONGet(ctx, "bikes:inventory", "$..[*].colors").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res21)
	// >>> [["black","silver","pink"],["black","silver","pink"],["black","white"]]
	// STEP_END

	// Output:
	// [3 3]
	// [["black","silver","pink"],["black","silver","pink"],["black","white"]]
}
