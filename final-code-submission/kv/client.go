package kv

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"time"

	"cs426.yale.edu/lab4/kv/proto"
	"github.com/sirupsen/logrus"
)

type Kv struct {
	shardMap   *ShardMap
	clientPool ClientPool

	// Add any client-side state you want here
}

func MakeKv(shardMap *ShardMap, clientPool ClientPool) *Kv {
	kv := &Kv{
		shardMap:   shardMap,
		clientPool: clientPool,
	}
	// Add any initialization logic
	return kv
}

// funciton that returns whether n1 and n2's vector clocks are concurrent
func isConcurrent(n1 map[string]int32, n2 map[string]int32, nodelist map[string]NodeInfo) bool {
	var smallerCounterExists bool
	var largerCounterExists bool

	smallerCounterExists = false
	largerCounterExists = false

	for node := range nodelist {
		smallerCounterExists = (n1[node] < n2[node]) || smallerCounterExists
		largerCounterExists = (n1[node]) > n2[node] || largerCounterExists
	}
	if smallerCounterExists && largerCounterExists {
		return true
	}
	return false
}

// function that returns whether n1 is an ancestor of n2
func isAncestor(n1 map[string]int32, n2 map[string]int32, nodelist map[string]NodeInfo) bool {
	var smallerCounterExists bool
	smallerCounterExists = false

	for node := range nodelist {
		smallerCounterExists = (n1[node] < n2[node]) || smallerCounterExists
	}
	return smallerCounterExists
}

func (kv *Kv) Get(ctx context.Context, key string) (string, bool, error) {
	// Trace-level logging -- you can remove or use to help debug in your tests
	// with `-log-level=trace`. See the logging section of the spec.
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Get() request")

	shard := GetShardForKey(key, kv.shardMap.NumShards())

	nodesForShard := kv.shardMap.NodesForShard(shard)

	if len(nodesForShard) == 0 {
		return "", false, fmt.Errorf("no nodes host this shard")
	}

	var allNodesFailed error

	// iterate through all nodes for shard, put responses into responseList
	responseList := make([]*proto.GetResponse, 0)

	for i := 0; i < len(nodesForShard); i++ {
		nodeName := nodesForShard[i]
		kvClient, err1 := kv.clientPool.GetClient(nodeName)
		if err1 != nil {
			allNodesFailed = err1
			continue
		}

		response, err2 := kvClient.Get(ctx, &proto.GetRequest{Key: key})

		if err2 != nil {
			allNodesFailed = err2
			continue
		}
		responseList = append(responseList, response)
	}

	// no valid responses, return error
	if len(responseList) == 0 {
		return "", false, allNodesFailed
	}

	// find list of most updated clocks from responseList
	updatedResponses := make([]*proto.GetResponse, 0)
	for _, response := range responseList {
		if len(updatedResponses) == 0 {
			updatedResponses = append(updatedResponses, response)
		} else {
			if isConcurrent(response.Clock, updatedResponses[0].Clock, kv.shardMap.Nodes()) || reflect.DeepEqual(response.Clock, updatedResponses[0].Clock) {
				updatedResponses = append(updatedResponses, response)
			} else if !isAncestor(response.Clock, updatedResponses[0].Clock, kv.shardMap.Nodes()) {
				updatedResponses = make([]*proto.GetResponse, 0)
				updatedResponses = append(updatedResponses, response)
			}
		}
	}

	randIdx := rand.Intn(len(updatedResponses))

	return updatedResponses[randIdx].Value, updatedResponses[randIdx].WasFound, nil

}

type lockedSlice struct {
	mu *sync.RWMutex
	sl []string
}

func (kv *Kv) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Set() request")

	shard := GetShardForKey(key, kv.shardMap.NumShards())

	nodesForShard := kv.shardMap.NodesForShard(shard)

	if len(nodesForShard) == 0 {
		return fmt.Errorf("no nodes host this shard")
	}

	var wg sync.WaitGroup
	var err error
	err = nil

	remainingNodes := lockedSlice{
		mu: &sync.RWMutex{},
		sl: kv.shardMap.NodesNotForShard(shard),
	}

	for _, node := range nodesForShard {
		wg.Add(1)
		go func(node string) {
			defer wg.Done()
			var err1 error
			kvClient, err1 := kv.clientPool.GetClient(node)

			if err1 != nil { // unable to reach client, try handoff to first healthy node
				remainingNodes.mu.Lock()
				if len(remainingNodes.sl) == 0 { // no available nodes to handoff, return err
					err = err1
					remainingNodes.mu.Unlock()
					return
				}
				// hinted handoff to first available healthy node
				var handoffErr error

				for i, nodeName := range remainingNodes.sl {
					cli, err3 := kv.clientPool.GetClient(nodeName)
					if err3 != nil {
						handoffErr = err3
						continue
					}
					// here we should have a new Temporary Set API for the temporary value stores
					cli.TmpSet(ctx, &proto.TmpSetRequest{
						Key:        key,
						Value:      value,
						TtlMs:      ttl.Milliseconds(),
						OriginNode: node},
					)

					// thread-safe removal of healthy element we just used to handoff from remainingNodes
					// so other goroutines can safely edit remainingNodes and we will guarantee
					// replication to N distinct nodes (if available)
					remainingNodes.sl[i] = remainingNodes.sl[len(remainingNodes.sl)-1]
					remainingNodes.sl = remainingNodes.sl[:len(remainingNodes.sl)-1]

					remainingNodes.mu.Unlock()
					return
				}
				// handoff failed to all nodes, set error
				remainingNodes.mu.Unlock()
				err = handoffErr
				return
			} else {
				var err2 error
				_, err2 = kvClient.Set(ctx, &proto.SetRequest{Key: key, Value: value, TtlMs: ttl.Milliseconds()})
				if err2 != nil { // we were able to reach client, but Set failed for some reason
					// err = err2

					remainingNodes.mu.Lock()
					if len(remainingNodes.sl) == 0 { // no available nodes to handoff, return err
						err = err1
						remainingNodes.mu.Unlock()
						return
					}
					// hinted handoff to first available healthy node
					var handoffErr error

					for i, nodeName := range remainingNodes.sl {
						cli, err3 := kv.clientPool.GetClient(nodeName)
						if err3 != nil {
							handoffErr = err3
							continue
						}
						// here we should have a new Temporary Set API for the temporary value stores
						cli.TmpSet(ctx, &proto.TmpSetRequest{
							Key:        key,
							Value:      value,
							TtlMs:      ttl.Milliseconds(),
							OriginNode: node},
						)

						// thread-safe removal of healthy element we just used to handoff from remainingNodes
						// so other goroutines can safely edit remainingNodes and we will guarantee
						// replication to N distinct nodes (if available)
						remainingNodes.sl[i] = remainingNodes.sl[len(remainingNodes.sl)-1]
						remainingNodes.sl = remainingNodes.sl[:len(remainingNodes.sl)-1]

						remainingNodes.mu.Unlock()
						return
					}
					// handoff failed to all nodes, set error
					remainingNodes.mu.Unlock()
					err = handoffErr

					return
				}
			}
		}(node)
	}
	wg.Wait()
	return err

}

func (kv *Kv) Delete(ctx context.Context, key string) error {
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Delete() request")

	shard := GetShardForKey(key, kv.shardMap.NumShards())

	nodesForShard := kv.shardMap.NodesForShard(shard)

	if len(nodesForShard) == 0 {
		return fmt.Errorf("no nodes host this shard")
	}

	var wg sync.WaitGroup
	var err error
	err = nil

	for _, node := range nodesForShard {
		wg.Add(1)
		go func(node string) {
			defer wg.Done()
			var err1 error
			kvClient, err1 := kv.clientPool.GetClient(node)
			if err1 != nil {
				err = err1
				return
			}
			var err2 error
			_, err2 = kvClient.Delete(ctx, &proto.DeleteRequest{Key: key})
			if err2 != nil {
				err = err2
				return
			}
		}(node)
	}
	wg.Wait()
	return err
}
