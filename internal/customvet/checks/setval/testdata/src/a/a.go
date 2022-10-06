package a

type GoodCmd struct {
	val int
}

func (c *GoodCmd) SetVal(val int) {
	c.val = val
}

func (c *GoodCmd) Result() (int, error) {
	return c.val, nil
}

type BadCmd struct {
	val int
}

func (c *BadCmd) Result() (int, error) { // want "\\*a.BadCmd is missing a SetVal method"
	return c.val, nil
}

type NotACmd struct {
	val int
}

func (c *NotACmd) Val() int {
	return c.val
}
