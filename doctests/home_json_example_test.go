// EXAMPLE: go_home_json
// HIDE_START
package example_commands_test

// HIDE_END
// STEP_START import
import (
	"context"
	"fmt"
	"sort"

	"github.com/redis/go-redis/v9"
)

// STEP_END

func ExampleClient_search_json() {
	// STEP_START connect
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
		Protocol: 2,
	})
	// STEP_END
	// REMOVE_START
	rdb.Del(ctx, "user:1", "user:2", "user:3")
	rdb.FTDropIndex(ctx, "idx:users")
	// REMOVE_END

	// STEP_START create_data
	user1 := map[string]interface{}{
		"name":  "Paul John",
		"email": "paul.john@example.com",
		"age":   42,
		"city":  "London",
	}

	user2 := map[string]interface{}{
		"name":  "Eden Zamir",
		"email": "eden.zamir@example.com",
		"age":   29,
		"city":  "Tel Aviv",
	}

	user3 := map[string]interface{}{
		"name":  "Paul Zamir",
		"email": "paul.zamir@example.com",
		"age":   35,
		"city":  "Tel Aviv",
	}
	// STEP_END

	// STEP_START make_index
	_, err := rdb.FTCreate(
		ctx,
		"idx:users",
		// Options:
		&redis.FTCreateOptions{
			OnJSON: true,
			Prefix: []interface{}{"user:"},
		},
		// Index schema fields:
		&redis.FieldSchema{
			FieldName: "$.name",
			As:        "name",
			FieldType: redis.SearchFieldTypeText,
		},
		&redis.FieldSchema{
			FieldName: "$.city",
			As:        "city",
			FieldType: redis.SearchFieldTypeTag,
		},
		&redis.FieldSchema{
			FieldName: "$.age",
			As:        "age",
			FieldType: redis.SearchFieldTypeNumeric,
		},
	).Result()

	if err != nil {
		panic(err)
	}
	// STEP_END

	// STEP_START add_data
	_, err = rdb.JSONSet(ctx, "user:1", "$", user1).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.JSONSet(ctx, "user:2", "$", user2).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.JSONSet(ctx, "user:3", "$", user3).Result()

	if err != nil {
		panic(err)
	}
	// STEP_END

	// STEP_START query1
	findPaulResult, err := rdb.FTSearch(
		ctx,
		"idx:users",
		"Paul @age:[30 40]",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(findPaulResult)
	// >>> {1 [{user:3 <nil> <nil> <nil> map[$:{"age":35,"city":"Tel Aviv"...
	// STEP_END

	// STEP_START query2
	citiesResult, err := rdb.FTSearchWithArgs(
		ctx,
		"idx:users",
		"Paul",
		&redis.FTSearchOptions{
			Return: []redis.FTSearchReturn{
				{
					FieldName: "$.city",
					As:        "city",
				},
			},
		},
	).Result()

	if err != nil {
		panic(err)
	}

	sort.Slice(citiesResult.Docs, func(i, j int) bool {
		return citiesResult.Docs[i].Fields["city"] < citiesResult.Docs[j].Fields["city"]
	})

	for _, result := range citiesResult.Docs {
		fmt.Println(result.Fields["city"])
	}
	// >>> London
	// >>> Tel Aviv
	// STEP_END

	// STEP_START query3
	aggOptions := redis.FTAggregateOptions{
		GroupBy: []redis.FTAggregateGroupBy{
			{
				Fields: []interface{}{"@city"},
				Reduce: []redis.FTAggregateReducer{
					{
						Reducer: redis.SearchCount,
						As:      "count",
					},
				},
			},
		},
	}

	aggResult, err := rdb.FTAggregateWithArgs(
		ctx,
		"idx:users",
		"*",
		&aggOptions,
	).Result()

	if err != nil {
		panic(err)
	}

	sort.Slice(aggResult.Rows, func(i, j int) bool {
		return aggResult.Rows[i].Fields["city"].(string) <
			aggResult.Rows[j].Fields["city"].(string)
	})

	for _, row := range aggResult.Rows {
		fmt.Printf("%v - %v\n",
			row.Fields["city"], row.Fields["count"],
		)
	}
	// >>> City: London - 1
	// >>> City: Tel Aviv - 2
	// STEP_END

	// Output:
	// {1 [{user:3 <nil> <nil> <nil> map[$:{"age":35,"city":"Tel Aviv","email":"paul.zamir@example.com","name":"Paul Zamir"}]}]}
	// London
	// Tel Aviv
	// London - 1
	// Tel Aviv - 2
}
