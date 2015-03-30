package redis

func (c *baseClient) Pool() pool {
	return c.connPool
}

func HashSlot(key string) int {
	return hashSlot(key)
}
