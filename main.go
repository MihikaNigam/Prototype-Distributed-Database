package main

import (
	"context"
	"fmt"
	"math/rand"

	// "os"
	"strconv"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	NODE_COUNT        = 100
	FAIL_TIME_SECONDS = 10
	KILL_COUNT        = NODE_COUNT/2 - 1
	uri               = "mongodb+srv://admin:admin@distributeddbcluster.dka3xcv.mongodb.net/?retryWrites=true&w=majority&appName=DistributedDBCluster"
)

func updateHashRingBasedOnGossip(hashRing *HashRing, table Membership) {
	for id, node := range table.members {
		if !node.alive {
			hashRing.RemoveServer(fmt.Sprintf("Server-%d", id))
		}
	}
}

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

	/*
	 * Gossip Protocol
	 * id - id of server
	 * gossipCh - channel array to communicate with other gossip routines
	 * gossipSync - used to sync when printing gossip table
	 */
	go gossip(id, gossipCh, gossipSync, gossipRaftChannels, gossipMainChannels, killChannels[id-1], hashRing, nodeCount)

	go raft(id, requestVoteChannels, voteResponseChannels, appendRequestChannels, appendResponseChannels, gossipRaftChannels, raftMainChannels, killRaftChannels[id-1], hashRing)

	myCh := channels[id-1]

	leader := -1
	// var trashArr = []int{1, 2, 3}
	// membershipTable := Membership{make(map[int]*Node), id, trashArr, hashRing}
	// membershipTable.initializeMembership(id, hashRing)

	for {
		select {

		// get new leader info from raft
		case newLeader := <-raftMainChannels[id-1]:
			leader = newLeader
			if id == leader {
				fmt.Println(id, "i am the leader")
			}

			//fmt.Println(id, "new leader -> ", leader)

		// Get new gossip data (if nodes are dead or alive)
		case table := <-gossipMainChannels[id-1]:
			// membershipTable = table
			updateHashRingBasedOnGossip(hashRing, table)

		case x := <-myCh:
			fmt.Println("Node", id, "received message from node", x)
			//wg.Done()
		}
	}

	// when we call Done() on wg, it signifies
	// that the go routine is done running
	wg.Done()
}

func main() {

	// This is used to wait for all the go routines
	// to run before the main function can exit
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

	//the main should like this starting both protocols and hashing is just to regulate/scale read writes into db
	clientOptions := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		fmt.Println("Connection Attempt to db failed ")
		fmt.Println(err)
		return
	}
	defer client.Disconnect(context.TODO())
	fmt.Println("Successfully connected to MongoDB!")

	hr := NewHashRing()

	//hardcoding for the time being instead of fetching from client
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
			logFile:        nil, // todo: assign a real file here
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
		}, 10)
	}

	NODE_COUNT := hr.NodeCount()

	// Instantiating the go routines (nodes)
	for i := 1; i < NODE_COUNT; i++ {
		alive = append(alive, i)
		go system(i, &wg, channels, gossipChannels, &gossipSync, voteRequestChannels, voteResponseChannels, appendRequestChannels, appendResponseChannels, gossipRaftChannels, gossipMainChannels, raftMainChannels, killChannels, killRaftChannels, hr, NODE_COUNT)
	}

	//simulating user requests
	fmt.Println("Simulating requests:")
	testUsers := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	for _, key := range testUsers {
		server := hr.GetServer(key)
		primaryDB, backupDB := server.database, server.backup
		if primaryDB == nil {
			primaryDB = backupDB
		}
		collection := primaryDB.Collection("Trees")
		var result bson.M
		if err := collection.FindOne(context.TODO(), bson.M{}).Decode(&result); err != nil {
			fmt.Println("Failed to fetch data:", err)
		} else {
			fmt.Println("Fetched data:", result)
		}
	}

	failTimer := time.Tick(FAIL_TIME_SECONDS * time.Second)
	deadCount := 1

	// Info for next node to kill
	aliveCount := NODE_COUNT
	randNode := rand.Intn(aliveCount)
	aliveCount--
	nodeToKill := alive[randNode]
	alive = append(alive[:randNode], alive[randNode+1:]...)
	fmt.Println("next dead", nodeToKill)

	for {
		select {
		case <-failTimer:
			if deadCount <= KILL_COUNT {

				killChannels[nodeToKill-1] <- 1
				killRaftChannels[nodeToKill-1] <- 1

				// Info for next node to kill
				randNode = rand.Intn(aliveCount)
				aliveCount--
				nodeToKill = alive[randNode]
				alive = append(alive[:randNode], alive[randNode+1:]...)
				//fmt.Println(alive)
				fmt.Println("next dead", nodeToKill)
				deadCount++
			} else {
				fmt.Println("killed enough")
			}
		}

	}
	wg.Wait()

	// fmt.Println("Distributed Database started")
	fmt.Println("Main exits\n")
}
