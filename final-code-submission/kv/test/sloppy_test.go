package kvtest

// integrated tests that check for correct functionality of sloppy quorum/hinted handoff procedure
// to have some weak consistency model in our KV store
import (
	"errors"
	"hash/fnv"
	"testing"
	"time"

	"cs426.yale.edu/lab4/kv"
	"github.com/stretchr/testify/assert"
)

func GetShardForKey(key string, numShards int) int {
	hasher := fnv.New32()
	hasher.Write([]byte(key))
	return int(hasher.Sum32())%numShards + 1
}

func TestSloppyBasic(t *testing.T) {
	setup := MakeTestSetup(
		kv.ShardMapState{
			NumShards: 2,
			Nodes:     makeNodeInfos(2),
			ShardsToNodes: map[int][]string{
				1: {"n1"},
				2: {"n2"},
			},
		},
	)

	s := GetShardForKey("abc", 2)

	// we remove the node that would originally host "abc"
	// we don't use RemoveNode since it updates the shardmap, which is unnecessary
	if s == 1 {
		setup.clientPool.OverrideGetClientError("n1", errors.New("server shut down"))
	} else {
		setup.clientPool.OverrideGetClientError("n2", errors.New("server shut down"))
	}

	// we set "abc", and this should not return an error since the other healthy node should be temporarily storing it
	err := setup.Set("abc", "123", 10*time.Second)
	assert.Nil(t, err)

	setup.Shutdown()
}

func TestSloppyMultiple(t *testing.T) {
	setup := MakeTestSetup(
		kv.ShardMapState{
			NumShards: 3,
			Nodes:     makeNodeInfos(5),
			ShardsToNodes: map[int][]string{
				1: {"n1", "n2", "n5"},
				2: {"n2", "n3", "n4", "n5"},
				3: {"n3", "n1", "n4"},
			},
		},
	)

	s := GetShardForKey("abc", 5)

	// we remove one of the nodes that would originally host "abc"
	if s == 1 {
		setup.clientPool.OverrideGetClientError("n1", errors.New("server shut down"))
	} else if s == 2 {
		setup.clientPool.OverrideGetClientError("n2", errors.New("server shut down"))
	} else {
		setup.clientPool.OverrideGetClientError("n3", errors.New("server shut down"))
	}

	// we set "abc", and this should not return an error since the other healthy node should be temporarily storing it
	err := setup.Set("abc", "123", 10*time.Second)
	assert.Nil(t, err)

	// we check that the other nodes that host this shard have successfully Set() "abc"
	if s == 1 {
		val, wasFound, err := setup.NodeGet("n2", "abc")
		assert.Nil(t, err)
		assert.True(t, wasFound)
		assert.Equal(t, val, "123")

		val, wasFound, err = setup.NodeGet("n5", "abc")
		assert.Nil(t, err)
		assert.True(t, wasFound)
		assert.Equal(t, val, "123")
	} else if s == 2 {
		val, wasFound, err := setup.NodeGet("n3", "abc")
		assert.Nil(t, err)
		assert.True(t, wasFound)
		assert.Equal(t, val, "123")

		val, wasFound, err = setup.NodeGet("n4", "abc")
		assert.Nil(t, err)
		assert.True(t, wasFound)
		assert.Equal(t, val, "123")

		val, wasFound, err = setup.NodeGet("n5", "abc")
		assert.Nil(t, err)
		assert.True(t, wasFound)
		assert.Equal(t, val, "123")
	} else if s == 3 {
		val, wasFound, err := setup.NodeGet("n1", "abc")
		assert.Nil(t, err)
		assert.True(t, wasFound)
		assert.Equal(t, val, "123")

		val, wasFound, err = setup.NodeGet("n4", "abc")
		assert.Nil(t, err)
		assert.True(t, wasFound)
		assert.Equal(t, val, "123")
	}

	setup.Shutdown()
}

func TestSloppyHandoffBasic(t *testing.T) {
	setup := MakeTestSetup(
		kv.ShardMapState{
			NumShards: 2,
			Nodes:     makeNodeInfos(2),
			ShardsToNodes: map[int][]string{
				1: {"n1"},
				2: {"n2"},
			},
		},
	)

	s := GetShardForKey("abc", 2)

	// we remove the node that would originally host "abc"
	if s == 1 {
		setup.clientPool.OverrideGetClientError("n1", errors.New("server shut down"))
	} else {
		setup.clientPool.OverrideGetClientError("n2", errors.New("server shut down"))
	}

	// we set "abc", should temp store in the node that doesn't host the shard
	err := setup.Set("abc", "123", 60*time.Second)
	assert.Nil(t, err)

	// origin node should not have the value
	// origin node should have the value
	if s == 1 {
		_, wasFound, err := setup.NodeGet("n1", "abc")
		assert.Nil(t, err)
		assert.False(t, wasFound)
	} else {
		_, wasFound, err := setup.NodeGet("n2", "abc")
		assert.Nil(t, err)
		assert.False(t, wasFound)
	}

	// we add the node back
	setup.clientPool.ClearGetClientErrors()

	// wait for temporary store garbage collector to send KV to origin node
	time.Sleep(5 * time.Second)

	// origin node should have the value
	if s == 1 {
		val, wasFound, err := setup.NodeGet("n1", "abc")
		assert.Nil(t, err)
		assert.True(t, wasFound)
		assert.Equal(t, "123", val)
	} else {
		val, wasFound, err := setup.NodeGet("n2", "abc")
		assert.Nil(t, err)
		assert.True(t, wasFound)
		assert.Equal(t, "123", val)
	}

	setup.Shutdown()
}

func TestSloppyVectorClockBasic(t *testing.T) {
	setup := MakeTestSetup(
		kv.ShardMapState{
			NumShards: 1,
			Nodes:     makeNodeInfos(2),
			ShardsToNodes: map[int][]string{
				1: {"n1", "n2"},
			},
		},
	)

	// we set "abc" to different values in the two nodes
	err := setup.NodeSet("n1", "abc", "123", 60*time.Second)
	assert.Nil(t, err)

	err = setup.NodeSet("n2", "abc", "456", 60*time.Second)
	assert.Nil(t, err)

	// we get "abc" 10 times; since n1 and n2 have concurrent writes, there's a very high probability
	// that we get 2 different values across the 10 Get calls
	var nodeOne bool
	var nodeTwo bool

	nodeOne = false
	nodeTwo = false

	for i := 0; i < 10; i++ {
		val, wasFound, err := setup.Get("abc")
		assert.Nil(t, err)
		assert.True(t, wasFound)
		if val == "123" {
			nodeOne = true
		}
		if val == "456" {
			nodeTwo = true
		}
	}
	assert.True(t, nodeOne)
	assert.True(t, nodeTwo)

	setup.Shutdown()
}

func TestSloppyVectorClockMultiple(t *testing.T) {
	setup := MakeTestSetup(
		kv.ShardMapState{
			NumShards: 1,
			Nodes:     makeNodeInfos(3),
			ShardsToNodes: map[int][]string{
				1: {"n1", "n2", "n3"},
			},
		},
	)
	// set "abc" to all 3 nodes
	err := setup.Set("abc", "123", 60*time.Second)
	assert.Nil(t, err)

	// shut down n1
	setup.clientPool.OverrideGetClientError("n1", errors.New("server shut down"))

	// update abc to 2 different values in n2 and n3
	err = setup.NodeSet("n2", "abc", "456", 60*time.Second)
	assert.Nil(t, err)

	err = setup.NodeSet("n3", "abc", "789", 60*time.Second)
	assert.Nil(t, err)

	// revive n1
	setup.clientPool.ClearGetClientErrors()

	nodeOne := false
	nodeTwo := false
	nodeThree := false

	// we should discard n1's value since it is an ancestor, but we should get either n2 or n3's values
	// 10 runs should be enough to almost guarantee that n2 and n3's values are both returned

	for i := 0; i < 10; i++ {
		val, wasFound, err := setup.Get("abc")
		assert.Nil(t, err)
		assert.True(t, wasFound)
		if val == "123" {
			nodeOne = true
		}
		if val == "456" {
			nodeTwo = true
		}
		if val == "789" {
			nodeThree = true
		}
	}
	assert.False(t, nodeOne)
	assert.True(t, nodeTwo)
	assert.True(t, nodeThree)

	setup.Shutdown()
}

func TestSloppyHandoffMultiple(t *testing.T) {
	setup := MakeTestSetup(
		kv.ShardMapState{
			NumShards: 10,
			Nodes:     makeNodeInfos(5),
			ShardsToNodes: map[int][]string{
				1:  {"n1", "n2", "n3"},
				2:  {"n4", "n5", "n1"},
				3:  {"n2", "n3", "n4"},
				4:  {"n5", "n1"},
				5:  {"n2", "n3"},
				6:  {"n4", "n5", "n1"},
				7:  {"n2", "n3"},
				8:  {"n4", "n5", "n1"},
				9:  {"n2", "n3", "n4"},
				10: {"n5", "n1"},
			},
		},
	)

	// shut down n1
	setup.clientPool.OverrideGetClientError("n1", errors.New("server shut down"))

	// we set "d" (which is assigned to shard 4), and this should not return an error since the other healthy node should be temporarily storing it
	err := setup.Set("d", "123", 10*time.Second)
	assert.Nil(t, err)

	// a bunch of other keys should never return errors, since no shard is assigned to only one node (n1)
	err = setup.Set("a", "123", 10*time.Second)
	assert.Nil(t, err)

	err = setup.Set("b", "123", 10*time.Second)
	assert.Nil(t, err)

	err = setup.Set("e", "123", 10*time.Second)
	assert.Nil(t, err)

	err = setup.Set("f", "123", 10*time.Second)
	assert.Nil(t, err)

	err = setup.Set("g", "123", 10*time.Second)
	assert.Nil(t, err)

	err = setup.Set("h", "123", 10*time.Second)
	assert.Nil(t, err)

	err = setup.Set("i", "123", 10*time.Second)
	assert.Nil(t, err)

	err = setup.Set("j", "123", 10*time.Second)
	assert.Nil(t, err)

	err = setup.Set("k", "123", 10*time.Second)
	assert.Nil(t, err)

	err = setup.Set("l", "123", 10*time.Second)
	assert.Nil(t, err)

	setup.Shutdown()
}
