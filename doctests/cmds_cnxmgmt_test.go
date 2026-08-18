// EXAMPLE: cmds_cnxmgmt
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// HIDE_END

func ExampleClient_cmd_auth() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})
	// REMOVE_START

	// REMOVE_END
	// STEP_START auth1
	// REMOVE_START
	rdb.ConfigSet(ctx, "requirepass", "temp_pass")
	// REMOVE_END
	authResult1, err := rdb.Conn().Auth(ctx, "temp_pass").Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(authResult1) // >>> OK

	authResult2, err := rdb.Conn().AuthACL(
		ctx, "default", "temp_pass",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(authResult2) // >>> OK
	// REMOVE_START
	confRes, err := rdb.ConfigSet(ctx, "requirepass", "").Result()
	fmt.Println(confRes)
	// REMOVE_END
	// STEP_END

	// STEP_START auth2
	// REMOVE_START
	res, err := rdb.ACLSetUser(ctx, "test-user", "on", ">strong_password", "+acl").Result()
	fmt.Println(res)
	// REMOVE_END
	authResult3, err := rdb.Conn().AuthACL(ctx,
		"test-user", "strong_password",
	).Result()

	if err != nil {
		panic(err)
	}

	fmt.Println(authResult3) // >>> OK
	// REMOVE_START
	rdb.ACLDelUser(ctx, "test-user")
	// REMOVE_END
	// STEP_END

	// Output:
	// OK
	// OK
	// OK
}
