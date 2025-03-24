// EXAMPLE: query_range
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

func ExampleClient_query_range() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
		Protocol: 2,
	})

	// HIDE_END
	// REMOVE_START
	rdb.FTDropIndex(ctx, "idx:bicycle")
	rdb.FTDropIndex(ctx, "idx:email")
	// REMOVE_END

	_, err := rdb.FTCreate(ctx, "idx:bicycle",
		&redis.FTCreateOptions{
			OnJSON: true,
			Prefix: []interface{}{"bicycle:"},
		},
		&redis.FieldSchema{
			FieldName: "$.brand",
			As:        "brand",
			FieldType: redis.SearchFieldTypeText,
		},
		&redis.FieldSchema{
			FieldName: "$.model",
			As:        "model",
			FieldType: redis.SearchFieldTypeText,
		},
		&redis.FieldSchema{
			FieldName: "$.description",
			As:        "description",
			FieldType: redis.SearchFieldTypeText,
		},
		&redis.FieldSchema{
			FieldName: "$.price",
			As:        "price",
			FieldType: redis.SearchFieldTypeNumeric,
		},
		&redis.FieldSchema{
			FieldName: "$.condition",
			As:        "condition",
			FieldType: redis.SearchFieldTypeTag,
		},
	).Result()

	if err != nil {
		panic(err)
	}

	exampleJsons := []map[string]interface{}{
		{
			"pickup_zone": "POLYGON((-74.0610 40.7578, -73.9510 40.7578, -73.9510 40.6678, " +
				"-74.0610 40.6678, -74.0610 40.7578))",
			"store_location": "-74.0060,40.7128",
			"brand":          "Velorim",
			"model":          "Jigger",
			"price":          270,
			"description": "Small and powerful, the Jigger is the best ride for the smallest of tikes! " +
				"This is the tiniest kids pedal bike on the market available without a coaster brake, the Jigger " +
				"is the vehicle of choice for the rare tenacious little rider raring to go.",
			"condition": "new",
		},
		{
			"pickup_zone": "POLYGON((-118.2887 34.0972, -118.1987 34.0972, -118.1987 33.9872, " +
				"-118.2887 33.9872, -118.2887 34.0972))",
			"store_location": "-118.2437,34.0522",
			"brand":          "Bicyk",
			"model":          "Hillcraft",
			"price":          1200,
			"description": "Kids want to ride with as little weight as possible. Especially " +
				"on an incline! They may be at the age when a 27.5'' wheel bike is just too clumsy coming " +
				"off a 24'' bike. The Hillcraft 26 is just the solution they need!",
			"condition": "used",
		},
		{
			"pickup_zone": "POLYGON((-87.6848 41.9331, -87.5748 41.9331, -87.5748 41.8231, " +
				"-87.6848 41.8231, -87.6848 41.9331))",
			"store_location": "-87.6298,41.8781",
			"brand":          "Nord",
			"model":          "Chook air 5",
			"price":          815,
			"description": "The Chook Air 5  gives kids aged six years and older a durable " +
				"and uberlight mountain bike for their first experience on tracks and easy cruising through " +
				"forests and fields. The lower  top tube makes it easy to mount and dismount in any " +
				"situation, giving your kids greater safety on the trails.",
			"condition": "used",
		},
		{
			"pickup_zone": "POLYGON((-80.2433 25.8067, -80.1333 25.8067, -80.1333 25.6967, " +
				"-80.2433 25.6967, -80.2433 25.8067))",
			"store_location": "-80.1918,25.7617",
			"brand":          "Eva",
			"model":          "Eva 291",
			"price":          3400,
			"description": "The sister company to Nord, Eva launched in 2005 as the first " +
				"and only women-dedicated bicycle brand. Designed by women for women, allEva bikes " +
				"are optimized for the feminine physique using analytics from a body metrics database. " +
				"If you like 29ers, try the Eva 291. It’s a brand new bike for 2022.. This " +
				"full-suspension, cross-country ride has been designed for velocity. The 291 has " +
				"100mm of front and rear travel, a superlight aluminum frame and fast-rolling " +
				"29-inch wheels. Yippee!",
			"condition": "used",
		},
		{
			"pickup_zone": "POLYGON((-122.4644 37.8199, -122.3544 37.8199, -122.3544 37.7099, " +
				"-122.4644 37.7099, -122.4644 37.8199))",
			"store_location": "-122.4194,37.7749",
			"brand":          "Noka Bikes",
			"model":          "Kahuna",
			"price":          3200,
			"description": "Whether you want to try your hand at XC racing or are looking " +
				"for a lively trail bike that's just as inspiring on the climbs as it is over rougher " +
				"ground, the Wilder is one heck of a bike built specifically for short women. Both the " +
				"frames and components have been tweaked to include a women’s saddle, different bars " +
				"and unique colourway.",
			"condition": "used",
		},
		{
			"pickup_zone": "POLYGON((-0.1778 51.5524, 0.0822 51.5524, 0.0822 51.4024, " +
				"-0.1778 51.4024, -0.1778 51.5524))",
			"store_location": "-0.1278,51.5074",
			"brand":          "Breakout",
			"model":          "XBN 2.1 Alloy",
			"price":          810,
			"description": "The XBN 2.1 Alloy is our entry-level road bike – but that’s " +
				"not to say that it’s a basic machine. With an internal weld aluminium frame, a full " +
				"carbon fork, and the slick-shifting Claris gears from Shimano’s, this is a bike which " +
				"doesn’t break the bank and delivers craved performance.",
			"condition": "new",
		},
		{
			"pickup_zone": "POLYGON((2.1767 48.9016, 2.5267 48.9016, 2.5267 48.5516, " +
				"2.1767 48.5516, 2.1767 48.9016))",
			"store_location": "2.3522,48.8566",
			"brand":          "ScramBikes",
			"model":          "WattBike",
			"price":          2300,
			"description": "The WattBike is the best e-bike for people who still " +
				"feel young at heart. It has a Bafang 1000W mid-drive system and a 48V 17.5AH " +
				"Samsung Lithium-Ion battery, allowing you to ride for more than 60 miles on one " +
				"charge. It’s great for tackling hilly terrain or if you just fancy a more " +
				"leisurely ride. With three working modes, you can choose between E-bike, " +
				"assisted bicycle, and normal bike modes.",
			"condition": "new",
		},
		{
			"pickup_zone": "POLYGON((13.3260 52.5700, 13.6550 52.5700, 13.6550 52.2700, " +
				"13.3260 52.2700, 13.3260 52.5700))",
			"store_location": "13.4050,52.5200",
			"brand":          "Peaknetic",
			"model":          "Secto",
			"price":          430,
			"description": "If you struggle with stiff fingers or a kinked neck or " +
				"back after a few minutes on the road, this lightweight, aluminum bike alleviates " +
				"those issues and allows you to enjoy the ride. From the ergonomic grips to the " +
				"lumbar-supporting seat position, the Roll Low-Entry offers incredible comfort. " +
				"The rear-inclined seat tube facilitates stability by allowing you to put a foot " +
				"on the ground to balance at a stop, and the low step-over frame makes it " +
				"accessible for all ability and mobility levels. The saddle is very soft, with " +
				"a wide back to support your hip joints and a cutout in the center to redistribute " +
				"that pressure. Rim brakes deliver satisfactory braking control, and the wide tires " +
				"provide a smooth, stable ride on paved roads and gravel. Rack and fender mounts " +
				"facilitate setting up the Roll Low-Entry as your preferred commuter, and the " +
				"BMX-like handlebar offers space for mounting a flashlight, bell, or phone holder.",
			"condition": "new",
		},
		{
			"pickup_zone": "POLYGON((1.9450 41.4301, 2.4018 41.4301, 2.4018 41.1987, " +
				"1.9450 41.1987, 1.9450 41.4301))",
			"store_location": "2.1734, 41.3851",
			"brand":          "nHill",
			"model":          "Summit",
			"price":          1200,
			"description": "This budget mountain bike from nHill performs well both " +
				"on bike paths and on the trail. The fork with 100mm of travel absorbs rough " +
				"terrain. Fat Kenda Booster tires give you grip in corners and on wet trails. " +
				"The Shimano Tourney drivetrain offered enough gears for finding a comfortable " +
				"pace to ride uphill, and the Tektro hydraulic disc brakes break smoothly. " +
				"Whether you want an affordable bike that you can take to work, but also take " +
				"trail in mountains on the weekends or you’re just after a stable, comfortable " +
				"ride for the bike path, the Summit gives a good value for money.",
			"condition": "new",
		},
		{
			"pickup_zone": "POLYGON((12.4464 42.1028, 12.5464 42.1028, " +
				"12.5464 41.7028, 12.4464 41.7028, 12.4464 42.1028))",
			"store_location": "12.4964,41.9028",
			"model":          "ThrillCycle",
			"brand":          "BikeShind",
			"price":          815,
			"description": "An artsy,  retro-inspired bicycle that’s as " +
				"functional as it is pretty: The ThrillCycle steel frame offers a smooth ride. " +
				"A 9-speed drivetrain has enough gears for coasting in the city, but we wouldn’t " +
				"suggest taking it to the mountains. Fenders protect you from mud, and a rear " +
				"basket lets you transport groceries, flowers and books. The ThrillCycle comes " +
				"with a limited lifetime warranty, so this little guy will last you long " +
				"past graduation.",
			"condition": "refurbished",
		},
	}

	for i, json := range exampleJsons {
		_, err := rdb.JSONSet(ctx, fmt.Sprintf("bicycle:%v", i), "$", json).Result()

		if err != nil {
			panic(err)
		}
	}

	// STEP_START range1
	res1, err := rdb.FTSearchWithArgs(ctx,
		"idx:bicycle", "@price:[500 1000]",
		&redis.FTSearchOptions{
			Return: []redis.FTSearchReturn{
				{
					FieldName: "price",
				},
			},
			SortBy: []redis.FTSearchSortBy{
				{
					FieldName: "price",
					Asc:       true,
				},
			},
		},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res1.Total) // >>> 3

	for _, doc := range res1.Docs {
		fmt.Printf("%v : price %v\n", doc.ID, doc.Fields["price"])
	}
	// >>> bicycle:2 : price 815
	// >>> bicycle:5 : price 810
	// >>> bicycle:9 : price 815
	// STEP_END

	// STEP_START range2
	res2, err := rdb.FTSearchWithArgs(ctx,
		"idx:bicycle", "*",
		&redis.FTSearchOptions{
			Filters: []redis.FTSearchFilter{
				{
					FieldName: "price",
					Min:       500,
					Max:       1000,
				},
			},
			Return: []redis.FTSearchReturn{
				{
					FieldName: "price",
				},
			},
			SortBy: []redis.FTSearchSortBy{
				{
					FieldName: "price",
					Asc:       true,
				},
			},
		},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res2.Total) // >>> 3

	for _, doc := range res2.Docs {
		fmt.Printf("%v : price %v\n", doc.ID, doc.Fields["price"])
	}
	// >>> bicycle:2 : price 815
	// >>> bicycle:5 : price 810
	// >>> bicycle:9 : price 815
	// STEP_END

	// STEP_START range3
	res3, err := rdb.FTSearchWithArgs(ctx,
		"idx:bicycle", "*",
		&redis.FTSearchOptions{
			Return: []redis.FTSearchReturn{
				{
					FieldName: "price",
				},
			},
			SortBy: []redis.FTSearchSortBy{
				{
					FieldName: "price",
					Asc:       true,
				},
			},
			Filters: []redis.FTSearchFilter{
				{
					FieldName: "price",
					Min:       "(1000",
					Max:       "+inf",
				},
			},
		},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res3.Total) // >>> 5

	for _, doc := range res3.Docs {
		fmt.Printf("%v : price %v\n", doc.ID, doc.Fields["price"])
	}
	// >>> bicycle:1 : price 1200
	// >>> bicycle:4 : price 3200
	// >>> bicycle:6 : price 2300
	// >>> bicycle:3 : price 3400
	// >>> bicycle:8 : price 1200
	// STEP_END

	// STEP_START range4
	res4, err := rdb.FTSearchWithArgs(ctx,
		"idx:bicycle",
		"@price:[-inf 2000]",
		&redis.FTSearchOptions{
			Return: []redis.FTSearchReturn{
				{
					FieldName: "price",
				},
			},
			SortBy: []redis.FTSearchSortBy{
				{
					FieldName: "price",
					Asc:       true,
				},
			},
			LimitOffset: 0,
			Limit:       5,
		},
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(res4.Total) // >>> 7

	for _, doc := range res4.Docs {
		fmt.Printf("%v : price %v\n", doc.ID, doc.Fields["price"])
	}
	// >>> bicycle:0 : price 270
	// >>> bicycle:7 : price 430
	// >>> bicycle:5 : price 810
	// >>> bicycle:2 : price 815
	// >>> bicycle:9 : price 815
	// STEP_END

	// Output:
	// 3
	// bicycle:5 : price 810
	// bicycle:2 : price 815
	// bicycle:9 : price 815
	// 3
	// bicycle:5 : price 810
	// bicycle:2 : price 815
	// bicycle:9 : price 815
	// 5
	// bicycle:1 : price 1200
	// bicycle:8 : price 1200
	// bicycle:6 : price 2300
	// bicycle:4 : price 3200
	// bicycle:3 : price 3400
	// 7
	// bicycle:0 : price 270
	// bicycle:7 : price 430
	// bicycle:5 : price 810
	// bicycle:2 : price 815
	// bicycle:9 : price 815
}
