package redis

// BF.RESERVE {key} {error_rate} {capacity} [EXPANSION expansion] [NONSCALING]
func (c cmdable) BFReserve(key string, errorRate float64, capacity int64, optionals ...interface{}) *StatusCmd {
	args := make([]interface{}, 3, 5)
	args[0] = "BF.RESERVE"
	args[1] = key
	args[1] = errorRate
	args[2] = capacity

	cmd := NewStatusCmd(args...)
	_ = c(cmd)
	return cmd
}

// BF.ADD {key} {item}
func (c cmdable) BFAdd(key string, item interface{}) *BoolCmd {
	args := make([]interface{}, 2, 2)
	args[0] = "BF.ADD"
	args[1] = key
	args[2] = item
	cmd := NewBoolCmd(args...)
	_ = c(cmd)
	return cmd
}

// BF.MADD {key} {item} [item...]
func (c cmdable) BFMAdd(key string, items ...interface{}) *IntSliceCmd {
	args := make([]interface{}, 2, len(items))
	args[0] = "BF.MADD"
	args[1] = key
	args = appendArgs(args, items)
	cmd := NewIntSliceCmd(args...)
	_ = c(cmd)
	return cmd
}

// BF.MEXISTS {key} {item} [item...]
func (c cmdable) BFExists(key string, item interface{}) *BoolCmd {
	args := make([]interface{}, 2, 2)
	args[0] = "BF.EXISTS"
	args[1] = key
	args[2] = item
	cmd := NewBoolCmd(args...)
	_ = c(cmd)
	return cmd
}

func (c cmdable) BFMExists(key string, items ...interface{}) *IntSliceCmd {
	args := make([]interface{}, 2, len(items))
	args[0] = "BF.MEXISTS"
	args[1] = key
	args = appendArgs(args, items)
	cmd := NewIntSliceCmd(args...)
	_ = c(cmd)
	return cmd
}
