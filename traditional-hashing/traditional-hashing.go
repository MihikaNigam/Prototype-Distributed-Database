package main

import (
	"context"
	"fmt"
	"hash/crc32"
	"math/rand"
	"strconv"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func hashKey(key string) int {
	return int(crc32.ChecksumIEEE([]byte(key)))
}

type Server struct {
	name     string
	database *mongo.Database
	backup   *mongo.Database
}

type TraditionalHash struct {
	servers []Server
}

func (th *TraditionalHash) AddServer(server Server) {
	th.servers = append(th.servers, server)
}

func (th *TraditionalHash) RemoveServer(serverName string) {
	var newServers []Server
	for _, server := range th.servers {
		if server.name != serverName {
			newServers = append(newServers, server)
		}
	}
	th.servers = newServers
}

func (th *TraditionalHash) GetServer(key string) *Server {
	index := hashKey(key) % len(th.servers)
	return &th.servers[index]
}

func main() {
	uri := "mongodb+srv://admin:admin@distributeddbcluster.dka3xcv.mongodb.net/?retryWrites=true&w=majority&appName=DistributedDBCluster"
	th := TraditionalHash{}
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

	for i := 0; i < 100; i++ {
		th.AddServer(Server{name: "Server-" + strconv.Itoa(i), database: client.Database(dbNames[0]), backup: client.Database(dbNames[1])})
	}

	// Simulate requests
	requestDistribution := make(map[string]int)
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key-%d", rand.Intn(1000000))
		server := th.GetServer(key)
		collection := server.database.Collection("Trees")
		var result bson.M
		if err := collection.FindOne(context.TODO(), bson.M{}).Decode(&result); err != nil {
			//fmt.Println("", err)
		} else {
			//fmt.Println("Fetched data:", result)
		}
		requestDistribution[server.name]++
	}

	fmt.Println("Uniform Distribution Test")
	// Display distribution
	for server, count := range requestDistribution {
		fmt.Printf("Server %s handled %d requests\n", server, count)
	}

	// fmt.Println("Scalability Test")
	// //simulate scalability
	// requestDistribution := make(map[string]int)
	// for i := 0; i < 10000; i++ {
	// 	key := fmt.Sprintf("key-%d", rand.Intn(1000000))
	// 	server := th.GetServer(key)
	// 	collection := server.database.Collection("Trees")
	// 	var result bson.M
	// 	if err := collection.FindOne(context.TODO(), bson.M{}).Decode(&result); err != nil {
	// 		//fmt.Println("", err)
	// 	} else {
	// 		//fmt.Println("Fetched data:", result)
	// 	}
	// 	requestDistribution[server.name]++
	// }
	// // Display distribution
	// for server, count := range requestDistribution {
	// 	fmt.Printf("Server %s handled %d requests\n", server, count)
	// }

	// // Simulate failure
	// fmt.Println("Scalability Test")
	// for i := 10; i <= 50; i += 10 {
	// 	th.AddServer(Server{name: "Server-" + strconv.Itoa(i), database: client.Database(dbNames[0]), backup: client.Database(dbNames[1])}, 100) // Add server with 100 virtual nodes
	// 	startTime := time.Now()

	// 	elapsedTime := time.Since(startTime)
	// 	fmt.Printf("Time taken with %d servers: %s\n", i, elapsedTime)
	// }
	// th.RemoveServer("Server-5")

	// // Display distribution post-failure
	// fmt.Println("Post-failure distribution:")
	// for server, count := range requestDistribution {
	// 	fmt.Printf("%s handled %d requests\n", server, count)
	// }
}
