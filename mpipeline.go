package redis

// Not thread-safe.
type MultiPipeline struct {
	commandable

	cmds   []Cmder
	client *ClusterClient
	closed bool
}

// MultiPipeline creates a new pipeline which is able to execute commands
// against multiple shards. Be aware that multi-pipelines are
// not able to follow redirects and you may receive errors if a node
// fails or if keys are moved before/during pipeline execution.
func (c *ClusterClient) MultiPipeline() *MultiPipeline {
	pipe := &MultiPipeline{
		client: c,
		cmds:   make([]Cmder, 0, 10),
	}
	pipe.commandable.process = pipe.process
	return pipe
}

func (c *MultiPipeline) process(cmd Cmder) {
	c.cmds = append(c.cmds, cmd)
}

func (c *MultiPipeline) Close() error {
	c.closed = true
	return nil
}

func (c *MultiPipeline) Discard() error {
	if c.closed {
		return errClosed
	}
	c.cmds = c.cmds[:0]
	return nil
}

// Exec always returns list of commands and error of the first failed
// command if any.
func (c *MultiPipeline) Exec() ([]Cmder, error) {
	if c.closed {
		return nil, errClosed
	}

	cmds := c.cmds
	c.cmds = make([]Cmder, 0, 10)
	if len(cmds) == 0 {
		return []Cmder{}, nil
	}

	c.client.cachemx.RLock()
	pipes := make(map[string]*Pipeline)
	for _, cmd := range cmds {
		hashSlot := HashSlot(cmd.clusterKey())
		addr := c.client.getMasterAddrBySlot(hashSlot)
		pipe, ok := pipes[addr]
		if !ok {
			pipe = c.client.getNodeClientByAddr(addr).Pipeline()
			pipes[addr] = pipe
		}
		pipe.process(cmd)
	}
	c.client.cachemx.RUnlock()

	var err error
	for _, pipe := range pipes {
		if _, e := pipe.Exec(); e != nil && err == nil {
			err = e
		}
	}
	return cmds, err
}
