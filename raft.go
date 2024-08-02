package main

import (
	"math/rand"
	"strconv"
	"strings"
	"time"
	//"math"
)

const (
	// raft states
	FOLLOWER  = 1
	CANDIDATE = 2
	LEADER    = 3

	// time between leader heartbeats
	LEADER_HB_TIME = 70
)

// request some entries be appended
type AppendRequest struct {
	term         int
	leaderId     int
	prevLogIndex int
	prevLogTerm  int
	//entries[]  // sends more than one only for efficiency?
	leaderCommit int
}

// response for append entries
type AppendResponse struct {
	term    int
	success bool
}

// request a vote
type VoteRequest struct {
	term         int
	candidateId  int
	lastLogIndex int
	lastLogTerm  int
}

// response for a vote
type VoteResponse struct {
	term        int
	voteGranted bool
}

// returns a random timeout duration
func randomTimeout() time.Duration {
	return time.Duration(rand.Intn(300-150) + 150)
}

// make given node a leader
func (s *Server) makeLeader() {
	//fmt.Println(this.id, "Became Leader")
	s.state = LEADER

	// re-initialize next and match indexes
	for i := range s.nextIndex {
		s.nextIndex[i] = 0
	}
	for i := range s.matchIndex {
		s.matchIndex[i] = 0
	}
}

// check if given node is a leader
func (s *Server) isLeader() bool {
	return s.state == LEADER
}

// check if given node is a candidate
func (s *Server) isCandidate() bool {
	return s.state == CANDIDATE
}

// check if given node is a follower
func (s *Server) isFollower() bool {
	return s.state == FOLLOWER
}

// return id of the given node's leader
func (s *Server) getLeader() int {
	return s.votedFor
}

// for a given node, set who it wants to vote for
func (s *Server) setLeader(newLeader int) {
	s.votedFor = newLeader
}

// make given node a candidate
func (s *Server) makeCandidate() {
	//fmt.Println(s.id, "Became Candidate")
	s.state = CANDIDATE
}

// make a given node a follower
func (s *Server) makeFollower() {
	//fmt.Println(s.id, "Became Follower")
	s.state = FOLLOWER
}

// increase the number of votes for a given node
func (s *Server) increaseVote() {
	s.votes++
}

// return the number of votes this node has
func (s *Server) getVotes() int {
	return s.votes
}

// check if a given node has won the election
func (s *Server) wonElection() bool {
	return s.votes > (NODE_COUNT / 2)
}

// increase the current term for a given node
func (s *Server) increaseTerm() {
	s.currentTerm++
}

// send vote request to every node
func (s *Server) sendVoteRequest(channels [NODE_COUNT]chan VoteRequest) {
	//fmt.Println(s.id, "Is Requesting a Vote")

	// vote for self
	s.votes = 1

	//todo: calculate these properly
	lastLogIndex := 0
	lastLogTerm := 0

	id, _ := strconv.Atoi(strings.TrimPrefix(s.name, "Server-"))
	// send a vote request to every channel
	for i, ch := range channels {
		if id != i+1 {
			requestVote := VoteRequest{s.currentTerm, id, lastLogIndex, lastLogTerm}
			ch <- requestVote
		}
	}
}

func (s *Server) sendVoteResponse(request VoteRequest, channels [NODE_COUNT]chan VoteResponse) {
	//fmt.Println(s.id, "Is Responding to Vote")

	response := VoteResponse{s.currentTerm, false}

	//update currentTerm and don't vote
	if s.currentTerm < request.term {
		s.currentTerm = request.term
		s.makeFollower()
		s.votedFor = -1

		// todo: does this check for an up-to-date log?
	} else if s.votedFor == -1 || s.votedFor == request.candidateId {
		// && (request.lastLogTerm > s.currentTerm || (request.lastLogTerm == s.currentTerm && request.lastLogIndex >= s.commitIndex)) {
		s.votedFor = request.candidateId
		response.voteGranted = true
	}

	// send response to candidate
	//fmt.Println(request.candidateId, "sent a request to ", s.id)
	channels[(request.candidateId)-1] <- response
}

// todo: needs to take in an entries array
func (s *Server) sendAppendRequest(channels [NODE_COUNT]chan AppendRequest) {
	//fmt.Println(s.id, "Is Sending Append Request")

	//todo: calculate these properly
	prevLogIndex := 0
	prevLogTerm := 0

	id, _ := strconv.Atoi(strings.TrimPrefix(s.name, "Server-"))
	// send an append request to every channel
	for i, ch := range channels {
		if id != i+1 {
			requestVote := AppendRequest{s.currentTerm, id, prevLogIndex, prevLogTerm, s.commitIndex}
			ch <- requestVote
		}
	}
}

func (s *Server) sendAppendResponse(request AppendRequest, channels [NODE_COUNT]chan AppendResponse) {
	//fmt.Println(s.id, "Is Responding to An Append Request")

	response := AppendResponse{s.currentTerm, false}

	//update currentTerm
	if s.currentTerm < request.term {
		s.currentTerm = request.term
		s.makeFollower()
		s.votedFor = -1

		channels[request.leaderId-1] <- response
		return
	}

	// become follower if candidate recieves append entries
	if s.state == CANDIDATE {
		s.makeFollower()

		channels[request.leaderId-1] <- response
		return
	}

	// todo: how tf do i check if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	// todo: delete conflicting entry and all that follow it
	// todo: append new entries not in log

	// todo: update commit index
	// if (request.leaderCommit > this.commitIndex){
	// 	// how do i get index of last new entry?
	// 	this.commitIndex = math.Min(request.leaderCommit, 0)
	// }

	channels[request.leaderId-1] <- response

}

// what a raft node runs
func raft(id int, voteRequestChannels [NODE_COUNT]chan VoteRequest, voteResponseChannels [NODE_COUNT]chan VoteResponse, appendRequestChannels [NODE_COUNT]chan AppendRequest, appendResponseChannels [NODE_COUNT]chan AppendResponse, gossipRaftChannels [NODE_COUNT]chan Membership, raftMainChannels [NODE_COUNT]chan int, killRaftChannels chan int, hashRing *HashRing) {
	server := hashRing.GetServer("Server-" + strconv.Itoa(id-1))
	// fmt.Println("IN RAFT for Servver id : ", id)
	candidateTimer := time.Tick(server.timeout * time.Millisecond)
	leaderHBTimer := time.Tick(LEADER_HB_TIME * time.Millisecond)
	//failTimer := time.Tick(1 * time.Second)

	for {
		select {
		//receive kill signal from main
		case <-killRaftChannels:
			server.alive = false
			// fmt.Println("dead raft", id)

		//case <- failTimer:
		//if(this.state == LEADER){
		//this.alive = false
		//fmt.Println(id, "dead")
		//}

		// receive updated membership table
		case table := <-gossipRaftChannels[id-1]:
			//case <- gossipRaftChannels[id - 1]:
			membershipTable := table
			server.alive = membershipTable.members["Server-"+strconv.Itoa(id)].alive
			for serverId, server := range table.members {
				if !server.alive {
					hashRing.RemoveServer(serverId)
					// fmt.Printf("Removed %s from hash ring due to failure\n", serverId)
				}
			}

		case <-candidateTimer:
			// fmt.Println("IN candidateTimer")

			// follower's timeout
			if server.isFollower() && server.alive {
				server.increaseTerm()
				server.makeCandidate()
				//fmt.Println(this.id, "Started Election")
				server.sendVoteRequest(voteRequestChannels)

				// candidate's timeout
			} else if server.isCandidate() {
				//fmt.Println(this.id, "Re-started Old Election")
				server.sendVoteRequest(voteRequestChannels)
			}

		// tell nodes to vote
		case vRq := <-server.voteRequest:
			// fmt.Println("VOTING")
			// go make node vote
			if server.alive {
				server.sendVoteResponse(vRq, voteResponseChannels)
			}

			// reset timer
			server.timeout = randomTimeout()
			candidateTimer = time.Tick(server.timeout * time.Millisecond)

		// response to a follower's vote
		case vRs := <-server.voteResponse:
			// fmt.Println("RESPONDING to VOTES")
			// update term if necessary
			if vRs.term > server.currentTerm {
				server.currentTerm = vRs.term
				server.makeFollower()
			} else {
				// increment vote
				if vRs.voteGranted {
					server.increaseVote()
				}

				//fmt.Println(this.id, "Number of Votes: ", this.votes)

				// check if won election
				if server.wonElection() && !server.isLeader() {
					server.makeLeader()

					// send new leader info to main system
					for _, ch := range raftMainChannels {
						ch <- id
					}

					// send empty appendEntries
					server.sendAppendRequest(appendRequestChannels)
				}
			}

		// make nodes append entries
		case aRq := <-server.appendRequest:
			// fmt.Println("Appending Entries")
			// make node append
			if server.alive {
				server.sendAppendResponse(aRq, appendResponseChannels)

				// reset timer
				server.timeout = randomTimeout()
				candidateTimer = time.Tick(server.timeout * time.Millisecond)
			}

		// react to nodes who appended their entries
		case aRs := <-server.appendResponse:
			// fmt.Println("Reacting to Entries")
			//fmt.Println(this.id, "Got Append Response")

			// update term if necessary
			if aRs.term > server.currentTerm {
				server.currentTerm = aRs.term
				server.makeFollower()
			}

		case <-leaderHBTimer:
			if server.isLeader() && server.alive {
				//fmt.Println(this.id, "Is Sending Heartbeat")

				server.sendAppendRequest(appendRequestChannels)

				// reset timer
				server.timeout = randomTimeout()
				candidateTimer = time.Tick(server.timeout * time.Millisecond)
			}
		}
	}
}
