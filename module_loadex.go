package redis

// ModuleLoadexConfig struct is used to specify the arguments for the MODULE LOADEX command of redis.
// `MODULE LOADEX path [CONFIG name value [CONFIG name value ...]] [ARGS args [args ...]]`
type ModuleLoadexConfig struct {
	Path string
	Conf map[string]interface{}
	Args []interface{}
}

func (c *ModuleLoadexConfig) ToArgs() []interface{} {
	var args = make([]interface{}, 3, len(c.Conf)*2+len(c.Args)+3)
	args[0] = "MODULE"
	args[1] = "LOADEX"
	args[2] = c.Path
	for k, v := range c.Conf {
		args = append(args, "CONFIG", k, v)
	}
	for _, arg := range c.Args {
		args = append(args, "ARGS", arg)
	}
	return args
}
