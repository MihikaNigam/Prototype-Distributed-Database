package main

import (
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

type Node struct {
	//each node has a hash representing its pos in HR and server it represents
	hash   int
	server *Server
}

type Server struct {
	name  string
	data  []int
	mutex sync.RWMutex
}

type HashRing struct {
	//each hash ring has multiple nodes/server
	mutex  sync.RWMutex
	nodes  []Node
	ncount int //no of virtual nodes in each server
}

func NewHashRing(vnodesPerNode int) *HashRing {
	return &HashRing{
		ncount: vnodesPerNode,
	}
}

// func (h *HashRing) hash(s string) uint32 {
// 	hh := fnv.New32a()
// 	hh.Write([]byte(s))
// 	return hh.Sum32()
// }

func (h *HashRing) addServer(s *Server) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	for i := 0; i < h.ncount; i++ {
		hash := int(crc32.ChecksumIEEE([]byte(s.name + "#" + strconv.Itoa(i))))
		h.nodes = append(h.nodes, Node{hash: int(hash), server: s})
	}
	//sorting for efficient lookup, slice for dynamic array
	sort.Slice(h.nodes, func(i, j int) bool {
		return h.nodes[i].hash < h.nodes[j].hash
	})
}

func (h *HashRing) removeServer(sname string) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	var newServerNode []Node
	for _, n := range h.nodes {
		if n.server.name != sname {
			newServerNode = append(newServerNode, n)
		}
	}
	h.nodes = newServerNode
}

func (h *HashRing) getServer(sname string) *Server {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if len(h.nodes) == 0 {
		return nil
	}
	hash := int(crc32.ChecksumIEEE([]byte(sname)))
	idx := sort.Search(len(h.nodes), func(i int) bool {
		return h.nodes[i].hash >= hash
	})
	if idx == len(h.nodes) {
		idx = 0
	}
	return h.nodes[idx].server
}

func (h *HashRing) setData(key string, data int) {
	server := h.getServer(key)
	if server != nil {
		server.mutex.Lock()
		server.data = append(server.data, data)
		server.mutex.Unlock()
	}
}

// Get retrieves a value for a given key from the appropriate server
func (h *HashRing) getData(key string) []int {
	server := h.getServer(key)
	if server != nil {
		server.mutex.RLock()
		data_arr := server.data
		server.mutex.RUnlock()
		return data_arr
	}
	return nil
}

func main() {
	//each hash ring has multiple servers
	hr := NewHashRing(10)

	server_keys := []string{"S1", "S2", "S3"}
	for _, key := range server_keys {
		hr.addServer(&Server{name: key, data: make([]int, 0)})
	}

	fmt.Println("Writing data")
	//Write data in nodes
	var wg sync.WaitGroup
	var k int = 0
	var kMutex sync.Mutex // Mutex to protect access to k

	for i := 0; i < 10; i++ {

		wg.Add(1) //increasing WG counter so that it knows it has to wait for this goroutine to complete

		go func(i int) {
			defer wg.Done() //go routine done and decreases counter by 1

			kMutex.Lock()
			if k == len(server_keys) {
				k = 0
			}
			var key = server_keys[k]
			k = (k + 1)
			kMutex.Unlock()

			fmt.Println("data stored in " + key + " : " + strconv.Itoa(i))
			hr.setData(key, i)
		}(i)
	}
	wg.Wait()

	// var readWg sync.WaitGroup
	// //Go routine to Read Data
	// for _, k := range server_keys {
	// 	readWg.Add(1)
	// 	go func(k string) {
	// 		fmt.Print("never came here")
	// 		var data []int = hr.getData(k)
	// 		if data != nil {
	// 			fmt.Printf("Server %s data : \n", k)
	// 			for _, d := range data {
	// 				fmt.Println(d)
	// 			}
	// 		} else {
	// 			fmt.Printf("Server %s has no data\n", k)
	// 		}
	// 	}(k)
	// }
	// readWg.Wait()

	fmt.Print("\n reading from a server")
	var sk = "S2"
	var data []int = hr.getData(sk)
	if data != nil {
		fmt.Printf("Server %s data : \n", sk)
		for _, d := range data {
			fmt.Println(d)
		}
	} else {
		fmt.Printf("Server %s has no data\n", k)
	}

}
