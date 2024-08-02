package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	NEIGHBOR_COUNT     = 3
	DEATH              = 7 // Cycles to determine when a Node is dead
	SEND_HB_SECONDS    = 3
	SEND_TABLE_SECONDS = 5
)

// this is for each node
type Membership struct {
	members   map[string]*Server
	id        string
	neighbors []string
	hashRing  *HashRing
}

// type Members struct{
// 	id int
// 	server *Server
// 	neighbours []string
// }
// type Neighbors struct {
// 	neighbors [NEIGHBOR_COUNT]int
// }

type FailingNode struct {
	cycleCount int
	failed     bool
}

// func (m Membership) isAlive(idToCheck string) bool {
// 	fmt.Println("Inside isAlive")
// 	if idToCheck == m.id {
// 		return false
// 	}
// 	fmt.Println("Checking if VNode is alive for id : ", idToCheck)
// 	_, exists := m.hashRing.HasNode(fmt.Sprintf("Server-%d", idToCheck))
// 	return m.members[idToCheck].alive && exists
// }

// func getRandomNeighbors(m Membership) []int {
// 	var neighbors []int
// 	neighborCount := 0

// 	for neighborCount < NEIGHBOR_COUNT {
// 		randID := rand.Intn(NODE_COUNT-1) + 1
// 		if m.isAlive(randID) {
// 			neighbors = append(neighbors, randID)
// 			neighborCount++
// 		}
// 	}
// 	return neighbors
// }

func sendTable(m *Membership, channels [NODE_COUNT]chan Membership) {
	// fmt.Println("Inside sendTable")
	neighbors := m.hashRing.GetNeighbors(m.id, NEIGHBOR_COUNT) //getRandomNeighbors(*m)
	for _, neighborName := range neighbors {
		if neighbor, err := strconv.Atoi(strings.TrimPrefix(neighborName, "Server-")); err == nil {
			ch := channels[neighbor]
			ch <- *m
		}
	}
}

func (m Membership) printMembers(id int, failingCycles map[int]*FailingNode) {
	// fmt.Println("Inside printMembers")
	// fmt.Println("Node:", id, "Neighbours:", m.neighbors)

	for _, server := range m.members { //i := 1; i < NODE_COUNT; i++ {
		// node := m.members[i]
		status := "is alive"
		failing := "not failing"

		if !server.alive {
			status = "is dead"
		}

		number, _ := strconv.Atoi(strings.Split(server.name, "-")[1])

		fmt.Printf("Server %d has hb %d, time %.1f and %s (%s) (failing cycles %d)\n", server.name, server.hbcounter, server.time, status, failing, failingCycles[number].cycleCount)

	}
}

func (m *Membership) initializeMembership(id string, hr *HashRing) {
	// fmt.Println("Inside initializeMembership")
	m.hashRing = hr
	m.id = id
	m.members = make(map[string]*Server)

	for _, vnode := range hr.vnodes {
		serverId := vnode.server.name
		if _, exists := m.members[serverId]; !exists { //adds 1 server node avoiding duplicates
			m.members[serverId] = vnode.server
		}
	}
	m.members[id].hbcounter += 1
}

func (s *Server) updateNode(other *Server) {
	// fmt.Println("Inside updateNode")
	s.hbcounter = other.hbcounter
	s.time = float64(time.Now().Unix())
	s.failing = false
	s.alive = other.alive
}

func updateCycles(failingCycles map[int]*FailingNode) {
	// fmt.Println("Inside updateCycles")
	for _, i := range failingCycles {
		if i.failed {
			i.cycleCount++
		} else {
			i.cycleCount = 0
		}
	}
}

func (m *Membership) compareTables(id string, m2 *Membership, failingNodeCycles map[int]*FailingNode) {
	// fmt.Println("Inside compareTables")
	var this, other *Server

	for i := 1; i < NODE_COUNT; i++ {
		idx := "Server-" + strconv.Itoa(i)
		// Don't let node compare itself
		if idx != id {
			this = m.members[idx]
			other = m2.members[idx]

			// the node we are comparing is alive,
			// and the hb is increasing
			if other.alive && (other.hbcounter > this.hbcounter) {
				//failingNodeCycles[i] = 0
				failingNodeCycles[i].failed = false
				this.updateNode(other)
			} else {
				// Node is failing, increase cycle count
				if this.failing && this.alive {
					failingNodeCycles[i].failed = true
					//failingNodeCycles[i]++
				}
				if this.time >= other.time && (this.hbcounter != 0) {
					this.failing = true
				}
				if failingNodeCycles[i].cycleCount >= DEATH {
					this.alive = false
				}
			}
		}
	}
}

func (m *Membership) incrementHB(id string) {
	// fmt.Println("Inside incrementHB membership")
	n := m.members[id]
	n.incrementHB()
}

func (n *Server) incrementHB() {
	// fmt.Println("Inside incrementHB")
	n.hbcounter++
	n.time = float64(time.Now().Unix())
}

func initializeFailingNodeCycles(nodeCount int) map[int]*FailingNode {
	// fmt.Println("Inside initializeFailingNodeCycles")
	m := make(map[int]*FailingNode)
	for i := 1; i < nodeCount+1; i++ {
		m[i] = &FailingNode{0, false}
		//m[i].cycleCount = 0
		//m[i].failing = false
	}
	return m
}

func gossip(id int, channels [NODE_COUNT]chan Membership, im *sync.Mutex, gossipRaftChannels [NODE_COUNT]chan Membership, gossipMainChannels [NODE_COUNT]chan Membership, killChannels chan int, hashRing *HashRing, nodeCount int) {

	// fmt.Println("IN GOSSIP for Servver id : ", id)

	membershipTable := Membership{} //Membership{make(map[int]*Node), id, trashArr, hashRing}
	myCh := channels[id-1]

	failingNodeCycles := initializeFailingNodeCycles(nodeCount)
	idx := "Server-" + strconv.Itoa(id-1)
	membershipTable.initializeMembership(idx, hashRing)

	self := membershipTable.members[idx]
	cycleCount := 0

	hbTimer := time.Tick(SEND_HB_SECONDS * time.Second)
	sendTableTimer := time.Tick(SEND_TABLE_SECONDS * time.Second)

	//failRandom := time.Tick(10 * time.Second)

	for {
		select {

		// receieve kill signal from main
		case <-killChannels:
			// fmt.Println("killChannels")
			self.alive = false
			hashRing.RemoveServer(self.name)
			fmt.Println(id, "dead")

		// Receiving membership table
		case m := <-myCh:
			// fmt.Println("Receiving membership table")
			membershipTable.compareTables(idx, &m, failingNodeCycles)
			cycleCount++

			// A full cycle has complete
			if cycleCount == NEIGHBOR_COUNT {
				membershipTable.neighbors = hashRing.GetNeighbors(idx, NEIGHBOR_COUNT)
				cycleCount = 0
				updateCycles(failingNodeCycles)

				//var tmp = make(map[int]bool)
				//membershipTable.printMembers(id, failingNodeCycles)

				for i := 1; i < NODE_COUNT; i++ {
					//node := membershipTable.members[i]
					//fmt.Println(node)
					//tmp[node.id] = node.alive

				}
				//fmt.Println(tmp)

				// Send new membership table data to raft protocol
				gossipRaftChannels[id-1] <- membershipTable
				gossipMainChannels[id-1] <- membershipTable
				//if(im.TryLock()){
				//fmt.Println()
				//membershipTable.printMembers(id, failingNodeCycles)
				//fmt.Println()
				//im.Unlock()
				//}
			}

			//im.Lock()
			//if(im.TryLock()){
			//fmt.Println()
			//membershipTable.printMembers(id, failingNodeCycles)
			//fmt.Println()
			//im.Unlock()
			//}
		case <-sendTableTimer:
			// fmt.Println("sendTableTimer")
			if self.alive {
				sendTable(&membershipTable, channels)
			}

		case <-hbTimer:
			// fmt.Println("hbTimer")
			if self.alive {
				membershipTable.incrementHB(idx)
			}
			//case <- failRandom:
			//if(id == 1){
			//self.alive = false
			//fmt.Println("NODE", id, "FAILED")
			//}
		}
	}
}
