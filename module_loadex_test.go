package redis_test

import (
	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

var _ = Describe("Loadex Config", func() {
	It("converts the configuration to a slice of arguments correctly", func() {
		conf := &redis.ModuleLoadexConfig{
			Path: "/path/to/your/module.so",
			Conf: map[string]interface{}{
				"param1": "value1",
				"param2": "value2",
				"param3": 3,
			},
			Args: []interface{}{
				"arg1",
				"arg2",
				3,
			},
		}

		args := conf.ToArgs()

		// Test if the arguments are in the correct order
		expectedArgs := []interface{}{
			"MODULE",
			"LOADEX",
			"/path/to/your/module.so",
			"CONFIG",
			"param1",
			"value1",
			"CONFIG",
			"param2",
			"value2",
			"CONFIG",
			"param3",
			3,
			"ARGS",
			"arg1",
			"ARGS",
			"arg2",
			"ARGS",
			3,
		}

		Expect(args).To(Equal(expectedArgs))
	})
})
