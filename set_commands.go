package redis

import (
	"context"

	"github.com/redis/go-redis/v9/internal/hashtag"
)

// SetCmdable is an interface for Redis set commands.
// Sets are unordered collections of unique strings.
type SetCmdable interface {
	SAdd(ctx context.Context, key string, members ...interface{}) *IntCmd
	SCard(ctx context.Context, key string) *IntCmd
	SDiff(ctx context.Context, keys ...string) *StringSliceCmd
	SDiffStore(ctx context.Context, destination string, keys ...string) *IntCmd
	SInter(ctx context.Context, keys ...string) *StringSliceCmd
	SInterCard(ctx context.Context, limit int64, keys ...string) *IntCmd
	SInterStore(ctx context.Context, destination string, keys ...string) *IntCmd
	SIsMember(ctx context.Context, key string, member interface{}) *BoolCmd
	SMIsMember(ctx context.Context, key string, members ...interface{}) *BoolSliceCmd
	SMembers(ctx context.Context, key string) *StringSliceCmd
	SMembersMap(ctx context.Context, key string) *StringStructMapCmd
	SMove(ctx context.Context, source, destination string, member interface{}) *BoolCmd
	SPop(ctx context.Context, key string) *StringCmd
	SPopN(ctx context.Context, key string, count int64) *StringSliceCmd
	SRandMember(ctx context.Context, key string) *StringCmd
	SRandMemberN(ctx context.Context, key string, count int64) *StringSliceCmd
	SRem(ctx context.Context, key string, members ...interface{}) *IntCmd
	SScan(ctx context.Context, key string, cursor uint64, match string, count int64) *ScanCmd
	SUnion(ctx context.Context, keys ...string) *StringSliceCmd
	SUnionStore(ctx context.Context, destination string, keys ...string) *IntCmd
}

// SAdd Redis `SADD key member [member ...]` command.
// Adds the specified members to the set stored at key.
// Specified members that are already members of this set are ignored.
// If key does not exist, a new set is created before adding the specified members.
//
// Returns the number of elements that were added to the set, not including all
// the elements already present in the set.
func (c cmdable) SAdd(ctx context.Context, key string, members ...interface{}) *IntCmd {
	args := make([]interface{}, 2, 2+len(members))
	args[0] = "sadd"
	args[1] = key
	args = appendArgs(args, members)
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// SCard Redis `SCARD key` command.
// Returns the set cardinality (number of elements) of the set stored at key.
// Returns 0 if key does not exist.
func (c cmdable) SCard(ctx context.Context, key string) *IntCmd {
	cmd := NewIntCmd(ctx, "scard", key)
	_ = c(ctx, cmd)
	return cmd
}

// SDiff Redis `SDIFF key [key ...]` command.
// Returns the members of the set resulting from the difference between the first set
// and all the successive sets.
// Keys that do not exist are considered to be empty sets.
//
// Returns a slice of members of the resulting set.
func (c cmdable) SDiff(ctx context.Context, keys ...string) *StringSliceCmd {
	args := make([]interface{}, 1+len(keys))
	args[0] = "sdiff"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := NewStringSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// SDiffStore Redis `SDIFFSTORE destination key [key ...]` command.
// Stores the members of the set resulting from the difference between the first set
// and all the successive sets into destination.
// If destination already exists, it is overwritten.
//
// Returns the number of elements in the resulting set.
func (c cmdable) SDiffStore(ctx context.Context, destination string, keys ...string) *IntCmd {
	args := make([]interface{}, 2+len(keys))
	args[0] = "sdiffstore"
	args[1] = destination
	for i, key := range keys {
		args[2+i] = key
	}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// SInter Redis `SINTER key [key ...]` command.
// Returns the members of the set resulting from the intersection of all the given sets.
// Keys that do not exist are considered to be empty sets.
// With one of the keys being an empty set, the resulting set is also empty.
//
// Returns a slice of members of the resulting set.
func (c cmdable) SInter(ctx context.Context, keys ...string) *StringSliceCmd {
	args := make([]interface{}, 1+len(keys))
	args[0] = "sinter"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := NewStringSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// SInterCard Redis `SINTERCARD numkeys key [key ...] [LIMIT limit]` command.
// Returns the cardinality of the set resulting from the intersection of all the given sets.
// Keys that do not exist are considered to be empty sets.
// With one of the keys being an empty set, the resulting set is also empty.
//
// The limit parameter sets an upper bound on the number of results returned.
// If limit is 0, no limit is applied.
//
// Returns the number of elements in the resulting set.
func (c cmdable) SInterCard(ctx context.Context, limit int64, keys ...string) *IntCmd {
	numKeys := len(keys)
	args := make([]interface{}, 4+numKeys)
	args[0] = "sintercard"
	args[1] = numKeys
	for i, key := range keys {
		args[2+i] = key
	}
	args[2+numKeys] = "limit"
	args[3+numKeys] = limit
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// SInterStore Redis `SINTERSTORE destination key [key ...]` command.
// Stores the members of the set resulting from the intersection of all the given sets
// into destination.
// If destination already exists, it is overwritten.
//
// Returns the number of elements in the resulting set.
func (c cmdable) SInterStore(ctx context.Context, destination string, keys ...string) *IntCmd {
	args := make([]interface{}, 2+len(keys))
	args[0] = "sinterstore"
	args[1] = destination
	for i, key := range keys {
		args[2+i] = key
	}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// SIsMember Redis `SISMEMBER key member` command.
// Returns if member is a member of the set stored at key.
// Returns true if the element is a member of the set, false if it is not a member
// or if key does not exist.
func (c cmdable) SIsMember(ctx context.Context, key string, member interface{}) *BoolCmd {
	cmd := NewBoolCmd(ctx, "sismember", key, member)
	_ = c(ctx, cmd)
	return cmd
}

// SMIsMember Redis `SMISMEMBER key member [member ...]` command.
// Returns whether each member is a member of the set stored at key.
// For each member, returns true if the element is a member of the set, false if it is not
// a member or if key does not exist.
//
// Returns a slice of booleans, one for each member, indicating membership.
func (c cmdable) SMIsMember(ctx context.Context, key string, members ...interface{}) *BoolSliceCmd {
	args := make([]interface{}, 2, 2+len(members))
	args[0] = "smismember"
	args[1] = key
	args = appendArgs(args, members)
	cmd := NewBoolSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// SMembers Redis `SMEMBERS key` command output as a slice.
// Returns all the members of the set value stored at key.
// Returns an empty slice if key does not exist.
//
// Returns a slice of all members of the set.
func (c cmdable) SMembers(ctx context.Context, key string) *StringSliceCmd {
	cmd := NewStringSliceCmd(ctx, "smembers", key)
	_ = c(ctx, cmd)
	return cmd
}

// SMembersMap Redis `SMEMBERS key` command output as a map.
// Returns all the members of the set value stored at key as a map.
// Returns an empty map if key does not exist.
//
// Returns a map where keys are the set members and values are empty structs.
func (c cmdable) SMembersMap(ctx context.Context, key string) *StringStructMapCmd {
	cmd := NewStringStructMapCmd(ctx, "smembers", key)
	_ = c(ctx, cmd)
	return cmd
}

// SMove Redis `SMOVE source destination member` command.
// Moves member from the set at source to the set at destination.
// This operation is atomic. In every given moment the element will appear to be a member
// of source or destination for other clients.
//
// Returns true if the element is moved, false if the element is not a member of source
// and no operation was performed.
func (c cmdable) SMove(ctx context.Context, source, destination string, member interface{}) *BoolCmd {
	cmd := NewBoolCmd(ctx, "smove", source, destination, member)
	_ = c(ctx, cmd)
	return cmd
}

// SPop Redis `SPOP key` command.
// Removes and returns one or more random members from the set value stored at key.
// This version returns a single random member.
//
// Returns the removed member, or nil if key does not exist.
func (c cmdable) SPop(ctx context.Context, key string) *StringCmd {
	cmd := NewStringCmd(ctx, "spop", key)
	_ = c(ctx, cmd)
	return cmd
}

// SPopN Redis `SPOP key count` command.
// Removes and returns one or more random members from the set value stored at key.
// This version returns up to count random members.
//
// Returns a slice of removed members. If key does not exist, returns an empty slice.
func (c cmdable) SPopN(ctx context.Context, key string, count int64) *StringSliceCmd {
	cmd := NewStringSliceCmd(ctx, "spop", key, count)
	_ = c(ctx, cmd)
	return cmd
}

// SRandMember Redis `SRANDMEMBER key` command.
// Returns a random member from the set value stored at key.
// This version returns a single random member without removing it.
//
// Returns the random member, or nil if key does not exist or the set is empty.
func (c cmdable) SRandMember(ctx context.Context, key string) *StringCmd {
	cmd := NewStringCmd(ctx, "srandmember", key)
	_ = c(ctx, cmd)
	return cmd
}

// SRandMemberN Redis `SRANDMEMBER key count` command.
// Returns an array of random members from the set value stored at key.
// This version returns up to count random members without removing them.
// When called with a positive count, returns distinct elements.
// When called with a negative count, allows for repeated elements.
//
// Returns a slice of random members. If key does not exist, returns an empty slice.
func (c cmdable) SRandMemberN(ctx context.Context, key string, count int64) *StringSliceCmd {
	cmd := NewStringSliceCmd(ctx, "srandmember", key, count)
	_ = c(ctx, cmd)
	return cmd
}

// SRem Redis `SREM key member [member ...]` command.
// Removes the specified members from the set stored at key.
// Specified members that are not a member of this set are ignored.
// If key does not exist, it is treated as an empty set and this command returns 0.
//
// Returns the number of members that were removed from the set, not including
// non-existing members.
func (c cmdable) SRem(ctx context.Context, key string, members ...interface{}) *IntCmd {
	args := make([]interface{}, 2, 2+len(members))
	args[0] = "srem"
	args[1] = key
	args = appendArgs(args, members)
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// SUnion Redis `SUNION key [key ...]` command.
// Returns the members of the set resulting from the union of all the given sets.
// Keys that do not exist are considered to be empty sets.
//
// Returns a slice of members of the resulting set.
func (c cmdable) SUnion(ctx context.Context, keys ...string) *StringSliceCmd {
	args := make([]interface{}, 1+len(keys))
	args[0] = "sunion"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := NewStringSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// SUnionStore Redis `SUNIONSTORE destination key [key ...]` command.
// Stores the members of the set resulting from the union of all the given sets
// into destination.
// If destination already exists, it is overwritten.
//
// Returns the number of elements in the resulting set.
func (c cmdable) SUnionStore(ctx context.Context, destination string, keys ...string) *IntCmd {
	args := make([]interface{}, 2+len(keys))
	args[0] = "sunionstore"
	args[1] = destination
	for i, key := range keys {
		args[2+i] = key
	}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// SScan Redis `SSCAN key cursor [MATCH pattern] [COUNT count]` command.
// Incrementally iterates the set elements stored at key.
// This is a cursor-based iterator that allows scanning large sets efficiently.
//
// Parameters:
//   - cursor: The cursor value for the iteration (use 0 to start a new scan)
//   - match: Optional pattern to match elements (empty string means no pattern)
//   - count: Optional hint about how many elements to return per iteration
//
// Returns a ScanCmd that can be used to iterate through all members of the set.
// Use the returned cursor from each iteration to continue scanning.
func (c cmdable) SScan(ctx context.Context, key string, cursor uint64, match string, count int64) *ScanCmd {
	args := []interface{}{"sscan", key, cursor}
	if match != "" {
		args = append(args, "match", match)
	}
	if count > 0 {
		args = append(args, "count", count)
	}
	cmd := NewScanCmd(ctx, c, args...)
	if hashtag.Present(match) {
		cmd.SetFirstKeyPos(4)
	}
	_ = c(ctx, cmd)
	return cmd
}
