package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"path/filepath"
	"sort"
	"time"

	"gopkg.in/yaml.v2"
)

var PORT string = ":30000"

var DELIMITER byte = 255

type Neigh struct {
	ID         byte   `yaml:"id"`
	Address    string `yaml:"address"`
	EdgeWeight int    `yaml:"edge_weight"`
}

type yamlConfig struct {
	ID         byte    `yaml:"id"`
	Address    string  `yaml:"address"`
	Neighbours []Neigh `yaml:"neighbours"`
}

// Commands definition
type Command byte

const (
	Connect     Command = 0
	NewFragment         = 1
	Report              = 2
	Accept              = 3
	Reject              = 4
	Merge               = 5
)

type Packet struct {
	Cmd  Command
	Data byte
}

func getLowestWeightNeighbour(node yamlConfig) Neigh {
	var neighbours = node.Neighbours
	sort.Slice(neighbours, func(i, j int) bool {
		return neighbours[i].EdgeWeight < neighbours[j].EdgeWeight
	})

	return neighbours[0]
}

// omg golang...
func contains(s []byte, val byte) bool {
	for _, v := range s {
		if v == val {
			return true
		}
	}
	return false
}

func initAndParseFileNeighbours(filename string) yamlConfig {
	fullpath, _ := filepath.Abs("./" + filename)
	yamlFile, err := ioutil.ReadFile(fullpath)

	if err != nil {
		panic(err)
	}

	var data yamlConfig

	err = yaml.Unmarshal([]byte(yamlFile), &data)
	if err != nil {
		panic(err)
	}

	return data
}

func myLog(localAdress string, message string) {
	fmt.Printf("[%s] : %s\n", localAdress, message)
}

func send(nodeAddress string, neighAddress string) {
	myLog(nodeAddress, "Sending message to "+neighAddress)
	outConn, err := net.Dial("tcp", neighAddress+PORT)
	if err != nil {
		log.Fatal(err)
		return
	}
	outConn.Write([]byte(nodeAddress))
	outConn.Close()
}

func sendCommand(neighAddress string, cmd Command, data byte) {
	outConn, err := net.Dial("tcp", neighAddress+PORT)
	if err != nil {
		log.Fatal(err)
		return
	}

	outConn.Write([]byte{byte(cmd), data, DELIMITER})
	outConn.Close()
}

func waitForCommandWithTimeout(ln net.Listener, timeout time.Duration) Packet {
	ch := make(chan []byte)
	go func() {
		conn, _ := ln.Accept()
		rcvd, _ := bufio.NewReader(conn).ReadBytes(DELIMITER)
		conn.Close()
		ch <- rcvd
	}()

	var pck Packet
	select {
	case rcv := <-ch:
		// Received command in specified delay
		pck.Cmd = Command(rcv[0])
		pck.Data = rcv[1]

	case <-time.After(timeout * time.Millisecond):
		fmt.Println("Timed out, exiting.")
	}

	return pck
}

func sendToAllNeighbours(node yamlConfig) {
	myLog(node.Address, "Sending message to all neighbours...")
	for _, neigh := range node.Neighbours {
		go send(node.Address, neigh.Address)
	}
}

func server(neighboursFilePath string, isStartingPoint bool) {
	// Load node config
	var node yamlConfig = initAndParseFileNeighbours(neighboursFilePath)

	// Listen for incomming connections
	myLog(node.Address, "Starting server .... and listening ...")
	ln, err := net.Listen("tcp", node.Address+PORT)
	if err != nil {
		log.Fatal(err)
		return
	}

	// Send connect to neightbours with lowest weight
	var lowest = getLowestWeightNeighbour(node)
	time.Sleep(2 * time.Second) // Wait some time for all nodes to be ready
	sendCommand(lowest.Address, Connect, node.ID)

	var ind int = 0
	var rcvdFrom []byte
	for ind < len(node.Neighbours) {
		var msg = waitForCommandWithTimeout(ln, 100)
		rcvdFrom = append(rcvdFrom, byte(msg.Data))
	}

	//var msg = waitForCommandWithTimeout(ln, 100)
	//rcvdFrom = append(rcvdFrom, byte(msg.Data))

	// Check if root of fragment tree
	if contains(rcvdFrom, lowest.ID) && (lowest.ID < node.ID) {
		myLog(node.Address, "I'm the fragment's root")
		fmt.Println(rcvdFrom)
	} else {
		// Do not root stuff
	}

	/*var reach bool = false
	var count int = 0

	myLog(node.Address, "Neighbours file parsing ...")
	myLog(node.Address, "Done")

	myLog(node.Address, "Starting algorithm ...")
	if isStartingPoint {
		myLog(node.Address, "This node is the starting point")
		reach = true
		go sendToAllNeighbours(node)
	}

	for count < len(node.Neighbours) {
		conn, _ := ln.Accept()
		message, _ := bufio.NewReader(conn).ReadString('\n')
		conn.Close()
		myLog(node.Address, "Message received : "+message)
		count += 1
		if !reach {
			myLog(node.Address, "First reception on this node")
			reach = true
			go sendToAllNeighbours(node)
		}
		myLog(node.Address, "Message received, count = "+strconv.Itoa(count)+", len neighbours = "+strconv.Itoa(len(node.Neighbours)))

	}
	myLog(node.Address, "Message received from all neighboors, ending algorithm")*/
}

func main() {
	//localadress := "127.0.0.1"
	go server("./nodes/node-1.yaml", false)
	go server("./nodes/node-2.yaml", false)
	go server("./nodes/node-3.yaml", false)
	go server("./nodes/node-4.yaml", false)
	go server("./nodes/node-5.yaml", false)
	go server("./nodes/node-6.yaml", false)
	go server("./nodes/node-7.yaml", false)
	go server("./nodes/node-8.yaml", false)
	time.Sleep(2 * time.Second) //Waiting all node to be ready
	//server("./nodes/node-1.yaml", true)
	time.Sleep(2 * time.Second) //Waiting all console return from nodes
}
