package shardkv

import (
	"fmt"
	"testing"
)

func TestKey2ShardIsDeterministicAndInRange(t *testing.T) {
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("key-%d", i)
		s := Key2Shard(k)
		if s < 0 || s >= NShards {
			t.Fatalf("Key2Shard(%q) = %d, outside [0,%d)", k, s, NShards)
		}
		if s2 := Key2Shard(k); s2 != s {
			t.Fatalf("Key2Shard(%q) returned %d then %d: it must be a pure function", k, s, s2)
		}
	}
}

// Keys with a shared prefix are the realistic case ("user:1", "user:2", ...).
// A hash that only looked at the first byte would put every one of them in a
// single shard; this checks they actually spread.
func TestKey2ShardSpreadsKeysWithACommonPrefix(t *testing.T) {
	const n = 10000
	counts := make([]int, NShards)
	for i := 0; i < n; i++ {
		counts[Key2Shard(fmt.Sprintf("user:%d", i))]++
	}
	mean := float64(n) / NShards
	for s, c := range counts {
		t.Logf("shard %d: %5d keys (%.2fx the mean)", s, c, float64(c)/mean)
		if float64(c) < 0.8*mean || float64(c) > 1.2*mean {
			t.Fatalf("shard %d holds %d of %d keys (mean %.0f): the hash is not spreading prefixed keys evenly", s, c, n, mean)
		}
	}
}

func TestStaticConfigIsDeterministicAndBalanced(t *testing.T) {
	groups := map[int][]string{7: {"a"}, 3: {"b"}, 5: {"c"}}
	first := NewStaticConfig(groups)
	for i := 0; i < 50; i++ { // map iteration order is random; the result must not be
		if got := NewStaticConfig(groups); got.Shards != first.Shards {
			t.Fatalf("NewStaticConfig gave different assignments for the same groups: %v vs %v", got.Shards, first.Shards)
		}
	}
	owned := map[int]int{}
	for _, gid := range first.Shards {
		owned[gid]++
	}
	for gid, n := range owned {
		if n < 3 || n > 4 {
			t.Fatalf("group %d owns %d of %d shards, want 3 or 4 (balanced)", gid, n, NShards)
		}
	}
	if first.Shards[0] != 3 || first.Shards[1] != 5 || first.Shards[2] != 7 {
		t.Fatalf("assignment should follow ascending group id: got %v", first.Shards[:3])
	}
}

func TestConfigValidateRejectsUnroutableConfigs(t *testing.T) {
	good := NewStaticConfig(map[int][]string{1: {"a"}, 2: {"b"}})
	if err := good.Validate(); err != nil {
		t.Fatalf("a good config was rejected: %v", err)
	}

	if err := (Config{}).Validate(); err == nil {
		t.Fatal("a config with no groups must be rejected")
	}

	dangling := good
	dangling.Shards[4] = 99 // a group that isn't in Groups
	if err := dangling.Validate(); err == nil {
		t.Fatal("a shard assigned to an unknown group must be rejected")
	}

	empty := NewStaticConfig(map[int][]string{1: {}, 2: {"b"}})
	if err := empty.Validate(); err == nil {
		t.Fatal("a group with no addresses must be rejected")
	}
}
