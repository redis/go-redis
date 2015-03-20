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

	pipes := &clusterPipelines{make(map[string]*Pipeline), c.client}

	// Assign commands to pipelines
	for _, cmd := range cmds {
		pipes.process(cmd)
	}
	pipes.exec()

	// Detect MOVED errors, try again
	for i := 0; i <= c.client.opt.getMaxRedirects(); i++ {
		for _, cmd := range cmds {
			if err := cmd.Err(); err != nil {
				if moved, _, addr := c.client.hasMoved(err); moved {
					cmd.reset()
					pipes.processOn(addr, cmd)
				}
			}
		}
		if pipes.len() == 0 {
			break
		}
		pipes.exec()
	}

	// Search for the first error
	for _, cmd := range cmds {
		if err := cmd.Err(); err != nil {
			return cmds, err
		}
	}
	return cmds, nil
}

// ------------------------------------------------------------------------

type clusterPipelines struct {
	pipes  map[string]*Pipeline
	client *ClusterClient
}

func (cp *clusterPipelines) fetch(addr string) *Pipeline {
	pipe, ok := cp.pipes[addr]
	if !ok {
		pipe = cp.client.getNodeClientByAddr(addr).Pipeline()
		cp.pipes[addr] = pipe
	}
	return pipe
}

func (cp *clusterPipelines) process(cmd Cmder) {
	hashSlot := HashSlot(cmd.clusterKey())

	cp.client.cachemx.RLock()
	addr := cp.client.getMasterAddrBySlot(hashSlot)
	cp.fetch(addr).process(cmd)
	cp.client.cachemx.RUnlock()
}

func (cp *clusterPipelines) processOn(addr string, cmd Cmder) {
	cp.client.cachemx.RLock()
	cp.fetch(addr).process(cmd)
	cp.client.cachemx.RUnlock()
}

func (cp *clusterPipelines) len() int {
	return len(cp.pipes)
}

func (cp *clusterPipelines) exec() {
	for addr, pipe := range cp.pipes {
		pipe.Exec()
		pipe.Close()
		delete(cp.pipes, addr)
	}
}
