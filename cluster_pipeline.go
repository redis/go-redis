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

func (pipe *ClusterPipeline) process(cmd Cmder) {
	pipe.cmds = append(pipe.cmds, cmd)
}

// Discard resets the pipeline and discards queued commands.
func (pipe *ClusterPipeline) Discard() error {
	if pipe.closed {
		return errClosed
	}
	pipe.cmds = pipe.cmds[:0]
	return nil
}

func (pipe *ClusterPipeline) Exec() (cmds []Cmder, retErr error) {
	if pipe.closed {
		return nil, errClosed
	}
	if len(pipe.cmds) == 0 {
		return []Cmder{}, nil
	}

	cmds = pipe.cmds
	pipe.cmds = make([]Cmder, 0, 10)

	cmdsMap := make(map[string][]Cmder)
	for _, cmd := range cmds {
		slot := hashSlot(cmd.clusterKey())
		addr := pipe.cluster.slotMasterAddr(slot)
		cmdsMap[addr] = append(cmdsMap[addr], cmd)
	}

	for attempt := 0; attempt <= pipe.cluster.opt.getMaxRedirects(); attempt++ {
		failedCmds := make(map[string][]Cmder)

		for addr, cmds := range cmdsMap {
			client, err := pipe.cluster.getClient(addr)
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

			failedCmds, err = pipe.execClusterCmds(cn, cmds, failedCmds)
			if err != nil {
				retErr = err
			}
			client.putConn(cn, err)
		}

		cmdsMap = failedCmds
	}

	return cmds, retErr
}

// Close marks the pipeline as closed
func (pipe *ClusterPipeline) Close() error {
	pipe.Discard()
	pipe.closed = true
	return nil
}

func (pipe *ClusterPipeline) execClusterCmds(
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
			pipe.cluster.lazyReloadSlots()
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
