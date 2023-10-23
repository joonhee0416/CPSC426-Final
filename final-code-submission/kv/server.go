package kv

import (
	"context"
	"sync"
	"time"

	"cs426.yale.edu/lab4/kv/proto"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type KvServerImpl struct {
	proto.UnimplementedKvServer
	nodeName string

	shardMap   *ShardMap
	listener   *ShardMapListener
	clientPool ClientPool
	shutdown   chan struct{}

	KvMap       map[int]shardStruct
	stopCleanUp chan struct{}
	ticker      *time.Ticker
	mu          *sync.RWMutex
	shardsAdded []int

	tmpMap       map[string]nodeStruct
	tmpTicker    *time.Ticker
	stopTmpCheck chan struct{}
}

type nodeStruct struct {
	mu   *sync.RWMutex
	dict map[string]state
}

type shardStruct struct {
	mu   *sync.RWMutex
	dict map[string]state
}

type state struct {
	val string
	ttl time.Time
	// vector clock with node/counter pairs
	clock map[string]int32
}

func (server *KvServerImpl) addShard(shard int) error {
	nodesForShard := server.shardMap.NodesForShard(shard)
	// no peers contain the shard, return
	if len(nodesForShard) == 0 {
		return status.Errorf(codes.NotFound, "no peers contain shard")
	}

	// Otherwise, call getShardContent() until success
	for _, node := range nodesForShard {
		if node == server.nodeName {
			continue
		}
		client, err1 := server.clientPool.GetClient(node)
		if err1 != nil {
			continue
		}

		response, err2 := client.GetShardContents(context.Background(), &proto.GetShardContentsRequest{Shard: int32(shard)})

		if err2 != nil {
			continue
		}

		server.KvMap[shard] = shardStruct{
			mu:   &sync.RWMutex{},
			dict: make(map[string]state),
		}
		server.KvMap[shard].mu.Lock()
		for _, v := range response.GetValues() {
			server.KvMap[shard].dict[v.GetKey()] = state{
				val: v.GetValue(),
				ttl: time.Now().Add(time.Duration(v.GetTtlMsRemaining()) * time.Millisecond),
			}
		}
		server.KvMap[shard].mu.Unlock()
		// logrus.Debugf("%s copied shard %d from %s", server.nodeName, shard, node)
		// fmt.Printf("%s copied shard %d from %s\n", server.nodeName, shard, node)
		return nil
	}

	// all peers failed, return error
	return status.Error(codes.NotFound, "GetClient or GetShardContents failed for all peers")
}

func (server *KvServerImpl) handleShardMapUpdate() {
	// TODO: Part C
	server.mu.Lock()
	defer server.mu.Unlock()

	newShardsToAdd := server.shardMap.ShardsForNode(server.nodeName)

	for _, shard := range newShardsToAdd {
		if !containsShard(server.shardsAdded, shard) {
			// if error, log error and init map for shard as empty
			errAdd := server.addShard(shard)
			if errAdd != nil {
				server.KvMap[shard] = shardStruct{
					mu:   &sync.RWMutex{},
					dict: make(map[string]state),
				}
			}
		}
	}

	for _, s := range server.shardsAdded {
		if !containsShard(newShardsToAdd, s) {
			// delete(server.KvMap, s)
			server.KvMap[s].mu.Lock()
			entry := server.KvMap[s]
			entry.dict = make(map[string]state)
			server.KvMap[s] = entry
			server.KvMap[s].mu.Unlock()
		}
	}

	server.shardsAdded = newShardsToAdd

}

func containsShard(shards []int, shardNum int) bool {
	for _, s := range shards {
		if s == shardNum {
			return true
		}
	}
	return false
}

func (server *KvServerImpl) shardMapListenLoop() {
	listener := server.listener.UpdateChannel()
	for {
		select {
		case <-server.shutdown:
			return
		case <-listener:
			server.handleShardMapUpdate()
		}
	}
}

func MakeKvServer(nodeName string, shardMap *ShardMap, clientPool ClientPool) *KvServerImpl {
	listener := shardMap.MakeListener()
	KvMap := make(map[int]shardStruct)

	for _, shardInt := range shardMap.ShardsForNode(nodeName) {
		KvMap[shardInt] = shardStruct{
			mu:   &sync.RWMutex{},
			dict: make(map[string]state),
		}
	}

	server := KvServerImpl{
		nodeName:     nodeName,
		shardMap:     shardMap,
		listener:     &listener,
		clientPool:   clientPool,
		shutdown:     make(chan struct{}),
		KvMap:        KvMap,
		stopCleanUp:  make(chan struct{}),
		ticker:       time.NewTicker(4 * time.Second),
		shardsAdded:  make([]int, 0),
		mu:           &sync.RWMutex{},
		tmpMap:       make(map[string]nodeStruct),
		stopTmpCheck: make(chan struct{}),
		tmpTicker:    time.NewTicker(2 * time.Second),
	}
	go server.shardMapListenLoop()
	server.handleShardMapUpdate()
	go server.checkTmp()

	go server.ttlChecking()
	return &server
}

func (server *KvServerImpl) checkTmp() {
	for {
		select {
		case <-server.tmpTicker.C:
			server.clearTmpMap()
		case <-server.stopTmpCheck:
			server.ticker.Stop()
			return
		}
	}
}

func (server *KvServerImpl) clearTmpMap() {
	// try connecting to origin node, if successful, we go through each of the KVs in info and send to original node
	// should check if TTL is exceeded, if so, we don't need to update and we can clear from map
	// should also update TTL (ttl - timeNow = newTTLMS) before sending Set
	server.mu.RLock()
	defer server.mu.RUnlock()

	for node, info := range server.tmpMap {
		cli, err := server.clientPool.GetClient(node)
		if err != nil { // if origin node still down, we skip
			continue
		}
		info.mu.Lock()
		for key, state := range info.dict {
			ttl := state.ttl
			if time.Now().After(ttl) { // ttl expired, no need to Set in origin node, cleanup
				delete(info.dict, key)
			}
			newTtl := time.Until(ttl).Milliseconds()
			_, err2 := cli.Set(context.Background(), &proto.SetRequest{Key: key, Value: state.val, TtlMs: newTtl})
			if err2 == nil { // origin node stored the value, cleanup
				delete(info.dict, key)
			}
		}
		info.mu.Unlock()
	}
}

func (server *KvServerImpl) ttlChecking() {
	for {
		select {
		case <-server.ticker.C:
			server.cleanup()
		case <-server.stopCleanUp:
			server.ticker.Stop()
			return
		}
	}
}

func (server *KvServerImpl) cleanup() {
	for _, shard := range server.KvMap {
		shard.mu.Lock()
		for key, state := range shard.dict {
			if time.Now().After(state.ttl) {
				delete(shard.dict, key)
			}
		}
		shard.mu.Unlock()
	}
}

func (server *KvServerImpl) Shutdown() {
	server.shutdown <- struct{}{}
	server.stopCleanUp <- struct{}{}
	server.stopTmpCheck <- struct{}{}
	server.listener.Close()
}

func (server *KvServerImpl) Get(
	ctx context.Context,
	request *proto.GetRequest,
) (*proto.GetResponse, error) {
	// Trace-level logging for node receiving this request (enable by running with -log-level=trace),
	// feel free to use Trace() or Debug() logging in your code to help debug tests later without
	// cluttering logs by default. See the logging section of the spec.
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("node received Get() request")

	key := request.GetKey()

	if key == "" {
		return &proto.GetResponse{}, status.Error(codes.InvalidArgument, "empty key")
	}

	shard := GetShardForKey(key, server.shardMap.NumShards())

	server.mu.RLock()
	defer server.mu.RUnlock()

	if !containsShard(server.shardsAdded, shard) {
		return &proto.GetResponse{}, status.Error(codes.NotFound, "node is not host")
	}

	server.KvMap[shard].mu.RLock()
	defer server.KvMap[shard].mu.RUnlock()

	state, keyIn := server.KvMap[shard].dict[key]

	if !keyIn {
		return &proto.GetResponse{Value: "", WasFound: false, Clock: nil}, nil
	}

	if time.Now().Before(state.ttl) {
		return &proto.GetResponse{Value: state.val, WasFound: true, Clock: state.clock}, nil
	}

	return &proto.GetResponse{Value: "", WasFound: false, Clock: nil}, nil

}

func (server *KvServerImpl) Set(
	ctx context.Context,
	request *proto.SetRequest,
) (*proto.SetResponse, error) {
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("node received Set() request")

	key := request.GetKey()

	if key == "" {
		return &proto.SetResponse{}, status.Error(codes.InvalidArgument, "empty key")
	}

	shard := GetShardForKey(key, server.shardMap.NumShards())

	server.mu.RLock()
	defer server.mu.RUnlock()

	if !containsShard(server.shardsAdded, shard) {
		return &proto.SetResponse{}, status.Error(codes.NotFound, "node is not host")
	}

	server.KvMap[shard].mu.Lock()
	defer server.KvMap[shard].mu.Unlock()

	var updatedClock map[string]int32

	_, ok := server.KvMap[shard].dict[key]

	if !ok {
		updatedClock = make(map[string]int32)
	} else {
		updatedClock = server.KvMap[shard].dict[key].clock
		updatedClock[server.nodeName]++
	}

	server.KvMap[shard].dict[key] = state{
		val:   request.GetValue(),
		ttl:   time.Now().Add(time.Duration(request.GetTtlMs()) * time.Millisecond),
		clock: updatedClock,
	}

	return &proto.SetResponse{}, nil
}

func (server *KvServerImpl) TmpSet(
	ctx context.Context,
	request *proto.TmpSetRequest,
) (*proto.TmpSetResponse, error) {
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("node received TmpSet() request")

	key := request.Key

	server.mu.Lock()
	defer server.mu.Unlock()

	_, ok := server.tmpMap[request.OriginNode]

	if !ok {
		server.tmpMap[request.OriginNode] = nodeStruct{
			mu:   &sync.RWMutex{},
			dict: make(map[string]state),
		}
	}

	server.tmpMap[request.OriginNode].mu.Lock()
	defer server.tmpMap[request.OriginNode].mu.Unlock()
	server.tmpMap[request.OriginNode].dict[key] = state{
		val: request.Value,
		ttl: time.Now().Add(time.Duration(request.GetTtlMs()) * time.Millisecond),
	}

	return &proto.TmpSetResponse{}, nil
}

func (server *KvServerImpl) Delete(
	ctx context.Context,
	request *proto.DeleteRequest,
) (*proto.DeleteResponse, error) {
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("node received Delete() request")

	key := request.GetKey()

	if key == "" {
		return &proto.DeleteResponse{}, status.Error(codes.InvalidArgument, "empty key")
	}

	shard := GetShardForKey(key, server.shardMap.NumShards())

	server.mu.RLock()
	defer server.mu.RUnlock()

	if !containsShard(server.shardsAdded, shard) {
		return &proto.DeleteResponse{}, status.Error(codes.NotFound, "node is not host")
	}

	server.KvMap[shard].mu.Lock()
	defer server.KvMap[shard].mu.Unlock()
	delete(server.KvMap[shard].dict, key)
	return &proto.DeleteResponse{}, nil

}

func (server *KvServerImpl) GetShardContents(
	ctx context.Context,
	request *proto.GetShardContentsRequest,
) (*proto.GetShardContentsResponse, error) {
	shard := int(request.GetShard())

	if !containsShard(server.shardsAdded, shard) {
		return &proto.GetShardContentsResponse{}, status.Error(codes.NotFound, "node is not host")
	}

	values := make([]*proto.GetShardValue, 0)

	server.KvMap[shard].mu.RLock()
	defer server.KvMap[shard].mu.RUnlock()

	for key, value := range server.KvMap[shard].dict {
		values = append(values, &proto.GetShardValue{
			Key:            key,
			Value:          value.val,
			TtlMsRemaining: time.Until(value.ttl).Milliseconds(),
		})
	}

	return &proto.GetShardContentsResponse{
		Values: values,
	}, nil

}
