package shardkv

import (
	"fmt"

	pool "github.com/aksharraikanti/distsys-lab/03-connection-pooling"
)

// ShardedClient routes each key to the group that owns its shard. It holds one
// pooled client per group (Stage 3), so connections to a group are reused
// across all keys that group owns.
//
// It is safe for concurrent use — each per-group client is (Stage 4 Day 1
// made them so), and the routing tables are read-only after construction.
// Because each group has its own client, and a client serializes its own
// writes (dedup requires one write at a time per ClientID), writes to
// DIFFERENT groups proceed in parallel; that parallelism is the whole point.
type ShardedClient struct {
	cfg     Config
	clients map[int]*pool.PooledClient // group id -> client for that group
}

// NewShardedClient builds a client for a fixed configuration. Every group in
// the config is dialed up front, so an unreachable group is an error here
// rather than a surprise on the first key that happens to route to it.
func NewShardedClient(cfg Config) (*ShardedClient, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	c := &ShardedClient{cfg: cfg, clients: make(map[int]*pool.PooledClient, len(cfg.Groups))}
	for gid, addrs := range cfg.Groups {
		pc, err := pool.NewPooledClient(addrs, 1, 4)
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("shardkv: dialing group %d: %w", gid, err)
		}
		c.clients[gid] = pc
	}
	return c, nil
}

// groupFor returns the client for the group owning key's shard.
func (c *ShardedClient) groupFor(key string) *pool.PooledClient {
	return c.clients[c.cfg.Shards[Key2Shard(key)]]
}

// Get returns the value for key, or "" if it has never been set. It blocks
// until the owning group answers — including through that group's own
// elections — and, if the group is wholly unreachable, until it comes back.
func (c *ShardedClient) Get(key string) string { return c.groupFor(key).Get(key) }

// Put sets key to value.
func (c *ShardedClient) Put(key, value string) { c.groupFor(key).Put(key, value) }

// Append appends value onto whatever key currently holds.
func (c *ShardedClient) Append(key, value string) { c.groupFor(key).Append(key, value) }

// Close releases every group's connections.
func (c *ShardedClient) Close() {
	for _, pc := range c.clients {
		pc.Close()
	}
}
