package redis

import (
	"context"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

func TestClusterShardsCmdIgnoresUnknownFields(t *testing.T) {
	// Simulate a CLUSTER SHARDS response containing unknown fields at both the
	// shard level and node level. The 'nodes' field contains a list of all nodes
	// within the shard. Each individual node is a map of attributes that describe
	// the node. Some attributes are optional and more attributes may be added in
	// the future. The client must tolerate unknown attributes gracefully.
	//
	// This response contains:
	// - 1 shard with an unknown top-level field "new-shard-field"
	// - 1 slot range [0, 5460]
	// - 1 node with an unknown field "new-node-field"
	reply := "*1\r\n" + // array of 1 shard
		"%4\r\n" + // shard map with 4 entries (slots, nodes, new-shard-field, another-future-field)
		// slots
		"$5\r\nslots\r\n" +
		"*2\r\n:0\r\n:5460\r\n" +
		// nodes
		"$5\r\nnodes\r\n" +
		"*1\r\n" + // 1 node
		"%5\r\n" + // node map with 5 entries (id, port, role, health + 1 unknown)
		"$2\r\nid\r\n$40\r\ne7d1eec13a26e40c197e55aaa34ab68efa3d5db5\r\n" +
		"$4\r\nport\r\n:6379\r\n" +
		"$4\r\nrole\r\n$6\r\nmaster\r\n" +
		"$6\r\nhealth\r\n$6\r\nonline\r\n" +
		"$14\r\nnew-node-field\r\n$11\r\nsome-value1\r\n" +
		// unknown shard-level field (string value)
		"$15\r\nnew-shard-field\r\n$11\r\nsome-value2\r\n" +
		// unknown shard-level field (integer value)
		"$20\r\nanother-future-field\r\n:42\r\n"

	cmd := NewClusterShardsCmd(context.Background())
	rd := proto.NewReader(strings.NewReader(reply))

	if err := cmd.readReply(rd); err != nil {
		t.Fatalf("readReply() returned unexpected error: %v", err)
	}

	if len(cmd.val) != 1 {
		t.Fatalf("expected 1 shard, got %d", len(cmd.val))
	}

	shard := cmd.val[0]

	if len(shard.Slots) != 1 || shard.Slots[0].Start != 0 || shard.Slots[0].End != 5460 {
		t.Fatalf("unexpected slots: %+v", shard.Slots)
	}

	if len(shard.Nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(shard.Nodes))
	}

	node := shard.Nodes[0]
	if node.ID != "e7d1eec13a26e40c197e55aaa34ab68efa3d5db5" {
		t.Errorf("node ID = %q, want e7d1eec13a26e40c197e55aaa34ab68efa3d5db5", node.ID)
	}
	if node.Port != 6379 {
		t.Errorf("node Port = %d, want 6379", node.Port)
	}
	if node.Role != "master" {
		t.Errorf("node Role = %q, want master", node.Role)
	}
	if node.Health != "online" {
		t.Errorf("node Health = %q, want online", node.Health)
	}
}

func TestClusterShardsCmdUnknownNodeFieldWithArrayValue(t *testing.T) {
	// Verify that unknown node fields with complex values (arrays) are skipped.
	reply := "*1\r\n" + // 1 shard
		"%2\r\n" + // shard map: slots + nodes
		"$5\r\nslots\r\n*2\r\n:0\r\n:16383\r\n" +
		"$5\r\nnodes\r\n" +
		"*1\r\n" + // 1 node
		"%3\r\n" + // node map: id, role, unknown-array-field
		"$2\r\nid\r\n$40\r\naaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\n" +
		"$4\r\nrole\r\n$7\r\nreplica\r\n" +
		"$13\r\nfuture-extras\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"

	cmd := NewClusterShardsCmd(context.Background())
	rd := proto.NewReader(strings.NewReader(reply))

	if err := cmd.readReply(rd); err != nil {
		t.Fatalf("readReply() returned unexpected error: %v", err)
	}

	node := cmd.val[0].Nodes[0]
	if node.ID != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Errorf("node ID = %q, want aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", node.ID)
	}
	if node.Role != "replica" {
		t.Errorf("node Role = %q, want replica", node.Role)
	}
}

func TestClusterShardsCmdAcceptsNullEndpoint(t *testing.T) {
	reply := "*1\r\n" +
		"%2\r\n" +
		"$5\r\nslots\r\n*2\r\n:0\r\n:16383\r\n" +
		"$5\r\nnodes\r\n*1\r\n" +
		"%4\r\n" +
		"$8\r\nendpoint\r\n_\r\n" +
		"$4\r\nport\r\n:6379\r\n" +
		"$4\r\nrole\r\n$6\r\nmaster\r\n" +
		"$6\r\nhealth\r\n$6\r\nonline\r\n"

	cmd := NewClusterShardsCmd(context.Background())
	if err := cmd.readReply(proto.NewReader(strings.NewReader(reply))); err != nil {
		t.Fatal(err)
	}
	if got := cmd.val[0].Nodes[0].Endpoint; got != "" {
		t.Fatalf("null endpoint=%q, want empty origin marker", got)
	}
}

func TestClusterShardsCmdRejectsOddSlotsArray(t *testing.T) {
	// The CLUSTER SHARDS "slots" value is a flat array of start/end integer
	// pairs, so its length is always even. readReply consumed it with a
	// `for k := 0; k < l; k += 2` loop that reads two ints per iteration
	// without checking l, so a malicious or MITM server declaring an odd
	// length made the final iteration read one int past the array. That stray
	// read fabricated a slot range from the next frame and desynced the pooled
	// connection: readReply returned nil and the following reply was consumed
	// as part of the slots array.
	//
	// Here the odd *3 slots array is followed by a separate :777 reply. On the
	// unpatched parser readReply succeeds, invents the slot range {200, 777}
	// from :777 and swallows it; the guard returns an error so the connection
	// is dropped instead of reused desynced.
	reply := "*1\r\n" + // 1 shard
		"%1\r\n" + // shard map with 1 field
		"$5\r\nslots\r\n" +
		"*3\r\n:0\r\n:100\r\n:200\r\n" + // odd-length slots array
		":777\r\n" // the next command's reply

	cmd := NewClusterShardsCmd(context.Background())
	if err := cmd.readReply(proto.NewReader(strings.NewReader(reply))); err == nil {
		t.Fatalf("odd-length slots array accepted; readReply returned nil (val=%+v)", cmd.val)
	}
}

func TestClusterShardsCmdAcceptsEvenSlotsArray(t *testing.T) {
	// A well-formed reply with multiple slot ranges still parses cleanly.
	reply := "*1\r\n" +
		"%1\r\n" +
		"$5\r\nslots\r\n*4\r\n:0\r\n:100\r\n:200\r\n:300\r\n"

	cmd := NewClusterShardsCmd(context.Background())
	if err := cmd.readReply(proto.NewReader(strings.NewReader(reply))); err != nil {
		t.Fatal(err)
	}
	got := cmd.val[0].Slots
	if len(got) != 2 || got[0] != (SlotRange{Start: 0, End: 100}) || got[1] != (SlotRange{Start: 200, End: 300}) {
		t.Fatalf("unexpected slots: %+v", got)
	}
}
