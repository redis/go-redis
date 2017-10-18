package redis

import (
	"context"
	"sync/atomic"
)

func (c *ClusterClient) DBSize(ctx context.Context) *IntCmd {
	cmd := NewIntCmd("dbsize")
	var size int64
	err := c.ForEachMaster(func(master *Client) error {
		n, err := master.DBSize(ctx).Result()
		if err != nil {
			return err
		}
		atomic.AddInt64(&size, n)
		return nil
	})
	if err != nil {
		cmd.setErr(err)
		return cmd
	}
	cmd.val = size
	return cmd
}
