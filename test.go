package main

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	NODE_COUNT        = 10
	FAIL_TIME_SECONDS = 10
	KILL_COUNT        = NODE_COUNT/2 - 1
)

func system(id int, wg *sync.WaitGroup, channels [NODE_COUNT]chan int, gossipCh [NODE_COUNT]chan Membership, gossipSync *sync.Mutex,
	requestVoteChannels [NODE_COUNT]chan VoteRequest,
	voteResponseChannels [NODE_COUNT]chan VoteResponse,
	appendRequestChannels [NODE_COUNT]chan AppendRequest,
	appendResponseChannels [NODE_COUNT]chan AppendResponse,
	gossipRaftChannels [NODE_COUNT]chan Membership,
	gossipMainChannels [NODE_COUNT]chan Membership,
	raftMainChannels [NODE_COUNT]chan int,
	killChannels [NODE_COUNT]chan int,
	killRaftChannels [NODE_COUNT]chan int,
	hashRing *HashRing,
	nodeCount int,
) {
	go gossip(id, gossipCh, gossipSync, gossipRaftChannels, gossipMainChannels, killChannels[id-1], hashRing, nodeCount)

	go raft(id, requestVoteChannels, voteResponseChannels, appendRequestChannels, appendResponseChannels, gossipRaftChannels, raftMainChannels, killRaftChannels[id-1], hashRing)

	// myCh := channels[id-1]

	// leader := -1
	// for {
	// 	select {

	// 	// get new leader info from raft
	// 	case newLeader := <-raftMainChannels[id-1]:
	// 		leader = newLeader
	// 		if id == leader {
	// 			fmt.Println(id, "i am the leader")
	// 		}

	// 		//fmt.Println(id, "new leader -> ", leader)

	// 	// Get new gossip data (if nodes are dead or alive)
	// 	case table := <-gossipMainChannels[id-1]:
	// 		// membershipTable = table
	// 		updateHashRingBasedOnGossip(hashRing, table)

	// 	case x := <-myCh:
	// 		// fmt.Println("Node", id, "received message from node", x)
	// 	}
	// }
	// wg.Done()
}

// Assuming you have these methods in your HashRing
func simulateUniformDistribution(hr *HashRing, numberOfRequests int) {
	serverCount := make(map[string]int)
	for i := 0; i < numberOfRequests; i++ {
		key := fmt.Sprintf("key-%d", rand.Intn(1000000)) // Random keys
		server := hr.GetServer(key)
		primaryDB := server.database
		if server.database == nil {
			primaryDB = server.backup
		}
		collection := primaryDB.Collection("Trees")
		var result bson.M
		if err := collection.FindOne(context.TODO(), bson.M{}).Decode(&result); err != nil {
			//fmt.Println("", err)
		} else {
			//fmt.Println("Fetched data:", result)
		}
		serverCount[server.name]++
	}

	for server, count := range serverCount {
		fmt.Printf("Server %s handled %d requests\n", server, count)
	}
}

func simulateScalabilityTest(hr *HashRing, start, end, step int, client *mongo.Client, dbNames []string) {
	for i := start; i <= end; i += step {
		hr.AddServer(Server{name: "Server-" + strconv.Itoa(i), database: client.Database(dbNames[0]), backup: client.Database(dbNames[1])}, 1) // Add server with 100 virtual nodes
		startTime := time.Now()
		simulateUniformDistribution(hr, 10000) // Simulate 10,000 requests
		elapsedTime := time.Since(startTime)
		fmt.Printf("Time taken with %d servers: %s\n", i, elapsedTime)
	}
}

func simulateNodeFailureRecovery(hr *HashRing, serverName string, client *mongo.Client, dbNames []string) {
	fmt.Println("Simulating failure...")
	hr.RemoveServer(serverName)           // Simulate server failure
	simulateUniformDistribution(hr, 5000) // Checking distribution post-failure

	fmt.Println("Simulating recovery...")
	hr.AddServer(Server{name: "Server-10", database: client.Database(dbNames[0]), backup: client.Database(dbNames[1])}, 100) // Recover the server
	simulateUniformDistribution(hr, 5000)                                                                                    // Checking distribution post-recovery
}

func main() {
	uri := "mongodb+srv://admin:admin@distributeddbcluster.dka3xcv.mongodb.net/?retryWrites=true&w=majority&appName=DistributedDBCluster"
	var wg sync.WaitGroup
	var gossipSync sync.Mutex
	var channels [NODE_COUNT]chan int
	var gossipChannels [NODE_COUNT]chan Membership
	var gossipRaftChannels [NODE_COUNT]chan Membership
	var gossipMainChannels [NODE_COUNT]chan Membership
	var raftMainChannels [NODE_COUNT]chan int
	var killChannels [NODE_COUNT]chan int
	var killRaftChannels [NODE_COUNT]chan int

	// raft channels
	var voteRequestChannels [NODE_COUNT]chan VoteRequest
	var voteResponseChannels [NODE_COUNT]chan VoteResponse
	var appendRequestChannels [NODE_COUNT]chan AppendRequest
	var appendResponseChannels [NODE_COUNT]chan AppendResponse

	for i := 0; i < NODE_COUNT; i++ {
		channels[i] = make(chan int)
		gossipChannels[i] = make(chan Membership, NEIGHBOR_COUNT)
		gossipRaftChannels[i] = make(chan Membership, NODE_COUNT)
		gossipMainChannels[i] = make(chan Membership, NODE_COUNT)
		raftMainChannels[i] = make(chan int)
		killChannels[i] = make(chan int)
		killRaftChannels[i] = make(chan int)

		//initialize raft channels
		voteRequestChannels[i] = make(chan VoteRequest, NODE_COUNT)
		voteResponseChannels[i] = make(chan VoteResponse, NODE_COUNT)
		appendRequestChannels[i] = make(chan AppendRequest, NODE_COUNT)
		appendResponseChannels[i] = make(chan AppendResponse, NODE_COUNT)
	}

	wg.Add(NODE_COUNT)

	var alive []int

	hr := NewHashRing()
	clientOptions := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		fmt.Println("Connection Attempt to db failed ")
		fmt.Println(err)
		return
	}
	defer client.Disconnect(context.TODO())
	fmt.Println("Successfully connected to MongoDB!")

	dbNames := []string{"NatureDb", "NatureDb_copy"}

	for i := 0; i < NODE_COUNT; i++ {
		hr.AddServer(Server{name: "Server-" + strconv.Itoa(i), database: client.Database(dbNames[0]), backup: client.Database(dbNames[1]),
			hbcounter: 0,
			time:      float64(time.Now().Unix()),
			alive:     true,
			failing:   false,

			state:          FOLLOWER,
			currentTerm:    0,
			votedFor:       -1,
			currentLeader:  -1,
			logFile:        nil,
			commitIndex:    0,
			lastApplied:    0,
			nextIndex:      make([]int, NODE_COUNT),
			matchIndex:     make([]int, NODE_COUNT),
			votes:          0,
			timeout:        randomTimeout(),
			voteRequest:    voteRequestChannels[i],
			voteResponse:   voteResponseChannels[i],
			appendRequest:  appendRequestChannels[i],
			appendResponse: appendResponseChannels[i],
		}, 100)
	}

	// Instantiating the go routines (nodes)
	for i := 1; i < NODE_COUNT; i++ {
		alive = append(alive, i)
		//go system(i, &wg, channels, gossipChannels, &gossipSync, voteRequestChannels, voteResponseChannels, appendRequestChannels, appendResponseChannels, gossipRaftChannels, gossipMainChannels, raftMainChannels, killChannels, killRaftChannels, hr, NODE_COUNT)
	}

	// fmt.Println("Uniform Distribution Test")
	// simulateUniformDistribution(hr, 10000) // Simulate 10,000 requests

	fmt.Println("Scalability Test")
	simulateScalabilityTest(hr, 10, 50, 10, client, dbNames) // Start with 10 servers, up to 50, adding 10 each time

	// fmt.Println("Node Failure and Recovery Test")
	// simulateNodeFailureRecovery(hr, "Server-5", client, dbNames)

}
