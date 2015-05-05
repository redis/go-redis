package redis

// ClusterPipeline is not thread-safe.
type ClusterPipeline struct {
	commandable

	cmds    []Cmder
	cluster *ClusterClient
	closed  bool
}

// Pipeline creates a new pipeline which is able to execute commands
// against multiple shards.
func (c *ClusterClient) Pipeline() *ClusterPipeline {
	pipe := &ClusterPipeline{
		cluster: c,
		cmds:    make([]Cmder, 0, 10),
	}
	pipe.commandable.process = pipe.process
	return pipe
}

func (c *ClusterPipeline) process(cmd Cmder) {
	c.cmds = append(c.cmds, cmd)
}

// Close marks the pipeline as closed
func (c *ClusterPipeline) Close() error {
	c.closed = true
	return nil
}

// Discard resets the pipeline and discards queued commands
func (c *ClusterPipeline) Discard() error {
	if c.closed {
		return errClosed
	}
	c.cmds = c.cmds[:0]
	return nil
}

func (c *ClusterPipeline) Exec() (cmds []Cmder, retErr error) {
	if c.closed {
		return nil, errClosed
	}
	if len(c.cmds) == 0 {
		return []Cmder{}, nil
	}

	cmds = c.cmds
	c.cmds = make([]Cmder, 0, 10)

	cmdsMap := make(map[string][]Cmder)
	for _, cmd := range cmds {
		slot := hashSlot(cmd.clusterKey())
		addrs := c.cluster.slotAddrs(slot)

		var addr string
		if len(addrs) > 0 {
			addr = addrs[0] // First address is master.
		}

		cmdsMap[addr] = append(cmdsMap[addr], cmd)
	}

	for attempt := 0; attempt <= c.cluster.opt.getMaxRedirects(); attempt++ {
		failedCmds := make(map[string][]Cmder)

		for addr, cmds := range cmdsMap {
			client, err := c.cluster.getClient(addr)
			if err != nil {
				setCmdsErr(cmds, err)
				retErr = err
				continue
			}

			cn, err := client.conn()
			if err != nil {
				setCmdsErr(cmds, err)
				retErr = err
				continue
			}

			failedCmds, err = c.execClusterCmds(cn, cmds, failedCmds)
			if err != nil {
				retErr = err
			}
			client.putConn(cn, err)
		}

		cmdsMap = failedCmds
	}

	return cmds, retErr
}

func (c *ClusterPipeline) execClusterCmds(
	cn *conn, cmds []Cmder, failedCmds map[string][]Cmder,
) (map[string][]Cmder, error) {
	if err := cn.writeCmds(cmds...); err != nil {
		setCmdsErr(cmds, err)
		return failedCmds, err
	}

	var firstCmdErr error
	for i, cmd := range cmds {
		err := cmd.parseReply(cn.rd)
		if err == nil {
			continue
		}
		if isNetworkError(err) {
			cmd.reset()
			failedCmds[""] = append(failedCmds[""], cmds[i:]...)
			break
		} else if moved, ask, addr := isMovedError(err); moved {
			c.cluster.lazyReloadSlots()
			cmd.reset()
			failedCmds[addr] = append(failedCmds[addr], cmd)
		} else if ask {
			cmd.reset()
			failedCmds[addr] = append(failedCmds[addr], NewCmd("ASKING"), cmd)
		} else if firstCmdErr == nil {
			firstCmdErr = err
		}
	}

	return failedCmds, firstCmdErr
}
