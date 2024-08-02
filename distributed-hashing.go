package main

import (
	//"fmt"

	"hash/crc32"
	"os"
	"sort"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
)

type Server struct {
	name     string
	database *mongo.Database
	backup   *mongo.Database

	//gossip vars
	hbcounter int
	time      float64
	alive     bool
	failing   bool

	// raft vars
	state          int // leader/candidate/follower
	currentTerm    int
	votedFor       int
	currentLeader  int      // id of current leader
	logFile        *os.File // log for this node
	commitIndex    int
	lastApplied    int
	nextIndex      []int // array for each node
	matchIndex     []int // array for each node
	votes          int
	timeout        time.Duration
	voteRequest    chan VoteRequest
	voteResponse   chan VoteResponse
	appendRequest  chan AppendRequest
	appendResponse chan AppendResponse
}

type VirtualNode struct {
	server *Server
	id     int
	hash   int
}

type HashRing struct {
	vnodes     []VirtualNode
	sortedKeys []int
	keysMap    map[int]VirtualNode
}

func NewHashRing() *HashRing {
	return &HashRing{
		keysMap: make(map[int]VirtualNode),
	}
}

func (hr *HashRing) AddServer(server Server, numVnodes int) {
	for i := 0; i < numVnodes; i++ {
		hash := int(crc32.ChecksumIEEE([]byte(server.name + "-" + strconv.Itoa(i))))
		vnode := VirtualNode{server: &server, id: i, hash: hash}

		hr.vnodes = append(hr.vnodes, vnode)
		hr.keysMap[vnode.hash] = vnode
		hr.sortedKeys = append(hr.sortedKeys, vnode.hash)
	}
	sort.Ints(hr.sortedKeys)
}

func (hr *HashRing) RemoveServer(serverName string) {
	var newVnodes []VirtualNode
	for _, key := range hr.sortedKeys {
		vnode := hr.keysMap[key]
		if vnode.server.name != serverName {
			newVnodes = append(newVnodes, vnode)
		}
	}
	hr.vnodes = newVnodes

	// Rebuild the keys map and sorted keys
	hr.vnodes = newVnodes
	hr.keysMap = make(map[int]VirtualNode)
	hr.sortedKeys = []int{}
	for _, vnode := range newVnodes {
		hr.keysMap[vnode.id] = vnode
		hr.sortedKeys = append(hr.sortedKeys, vnode.hash)
	}
	sort.Ints(hr.sortedKeys)
}

func (hr *HashRing) FindServer(key string) int {
	if len(hr.sortedKeys) == 0 {
		return -1
	}
	hash := int(crc32.ChecksumIEEE([]byte(key)))
	idx := sort.Search(len(hr.sortedKeys), func(i int) bool { return hr.sortedKeys[i] >= hash })
	if idx == len(hr.sortedKeys) {
		idx = 0
	}
	return idx
}

func (hr *HashRing) GetServer(key string) *Server {
	idx := hr.FindServer(key)
	server := hr.keysMap[hr.sortedKeys[idx]].server
	return server
}

func (hr *HashRing) GetNeighbors(key string, count int) []string {
	if count < 1 {
		return nil
	}

	//find the server you want neighbours of
	idx := hr.FindServer(key)

	if idx == -1 {
		return nil
	}

	neighbors := make([]string, count) //creating array of 'count' number of neighbours

	// Get 'count' number of neighbors starting from the found index
	for i := 0; i < count; i++ {
		neighborIdx := (idx + i + 1) % len(hr.sortedKeys) //wrapping to beginning of ring after iindex reaches end
		serverName := hr.keysMap[hr.sortedKeys[neighborIdx]].server.name
		neighbors = append(neighbors, serverName)
	}

	return neighbors
}

// checking if server exists
func (hr *HashRing) HasNode(serverName string) (*VirtualNode, bool) {
	for _, vnode := range hr.vnodes {
		if vnode.server.name == serverName {
			return &vnode, true
		}
	}
	return nil, false
}

func (hr *HashRing) NodeCount() int {
	distinctServers := make(map[string]bool)
	for _, vnode := range hr.vnodes {
		distinctServers[vnode.server.name] = true
	}
	return len(distinctServers)
}
