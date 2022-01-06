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

const NODES_MAX = 20

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
	Ack                 = 6
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

func all(s []byte, val byte) bool {
	for _, v := range s {
		if v != val {
			return false
		}
	}
	return true
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
	enabled := true

	if enabled {
		fmt.Printf("[%s] : %s\n", localAdress, message)
	}
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

// Can be run with go keyword. Result will be pushed to ch
func waitForCommandWithTimeout(ln net.Listener, timeout time.Duration, ch chan<- Packet) {
	chTimeout := make(chan []byte)
	go func() {
		conn, _ := ln.Accept()
		rcvd, _ := bufio.NewReader(conn).ReadBytes(DELIMITER)
		conn.Close() // TODO: Check fi no need to defer to ensure closing
		chTimeout <- rcvd
	}()

	var pck Packet
	select {
	case rcv := <-chTimeout:
		// Received command in specified delay
		pck.Cmd = Command(rcv[0])
		pck.Data = rcv[1]

	case <-time.After(timeout * time.Millisecond):
		// fmt.Println("Timed out, exiting.")
	}

	ch <- pck
}

func waitForCommand(ln net.Listener) Packet {
	conn, _ := ln.Accept()
	rcvd, _ := bufio.NewReader(conn).ReadBytes(DELIMITER)
	conn.Close()

	var pck Packet
	pck.Cmd = Command(rcvd[0])
	pck.Data = rcvd[1]
	return pck
}

func sendToAllNeighbours(node yamlConfig, cmd Command, data byte) {
	myLog(node.Address, "Sending message to all neighbours...")
	for _, neigh := range node.Neighbours {
		go send(node.Address, neigh.Address)
	}
}

func sendToChilds(node yamlConfig, childs []byte, cmd Command, data byte) {
	// sendCommand(node.Neighbours[0].Address, NewFragment, 3)	// TODO: remove
	for _, neigh := range node.Neighbours {
		if contains(childs, neigh.ID) {
			sendCommand(neigh.Address, cmd, data)
		}
	}
}

func sendToParent(node yamlConfig, parentId byte, cmd Command, data byte) {
	for _, neigh := range node.Neighbours {
		if neigh.ID == parentId {
			sendCommand(neigh.Address, cmd, data)
		}
	}
}

func server(neighboursFilePath string, isStartingPoint bool) {
	// Load node config
	var node yamlConfig = initAndParseFileNeighbours(neighboursFilePath)

	// Listen for incomming connections
	// myLog(node.Address, "Starting server .... and listening ...")
	ln, err := net.Listen("tcp", node.Address+PORT)
	if err != nil {
		log.Fatal(err)
		return
	}

	// Send connect to neightbours with lowest weight
	var lowest = getLowestWeightNeighbour(node)
	time.Sleep(2 * time.Second) // Wait some time for all nodes to be ready
	sendCommand(lowest.Address, Connect, node.ID)

	// Wait for answer from each node neighbour
	var connectRcvdIds []byte
	var chRcv [NODES_MAX]chan Packet
	for ind := 0; ind < len(node.Neighbours); ind++ {
		chRcv[ind] = make(chan Packet)
		go waitForCommandWithTimeout(ln, 1000, chRcv[ind])
	}

	for ind := 0; ind < len(node.Neighbours); ind++ {
		pck := <-chRcv[ind]
		if pck.Data > 0 { // 0 means that no connect was sent from this neightbour (timeout)
			connectRcvdIds = append(connectRcvdIds, byte(pck.Data))
		}
	}

	// Check if root of fragment tree
	var myFragment byte
	var myChilds []byte
	if contains(connectRcvdIds, lowest.ID) && (lowest.ID > node.ID) {
		myLog(node.Address, "I'm the fragment's root")
		myFragment = node.ID

		// Send NewFragment with my ID to all childs
		time.Sleep(2 * time.Second)
		sendToChilds(node, connectRcvdIds, NewFragment, node.ID)
		sendToChilds(node, connectRcvdIds, NewFragment, node.ID)
		sendToChilds(node, connectRcvdIds, NewFragment, node.ID)
		sendToChilds(node, connectRcvdIds, NewFragment, node.ID) // TODO: check if with one call cant send
		myLog(node.Address, "Sent NewFragment")
	} else {
		// Wait to receive new fragment from parent
		myLog(node.Address, "Wait for NewFragment")

		// Remove root ID from myChilds
		for ind := 0; ind < len(connectRcvdIds); ind++ {
			if connectRcvdIds[ind] != lowest.ID {
				myChilds = append(myChilds, connectRcvdIds[ind])
			}
		}

		myParent := lowest.ID

		pck := waitForCommand(ln)
		myLog(node.Address, "Recieved smth")

		if pck.Cmd == NewFragment {
			myLog(node.Address, "NewFragment recieved")
			myFragment = pck.Data
			sendToChilds(node, myChilds, NewFragment, myFragment)

			// Wait for acknowledge from all childs
			var acks []byte
			var chRcv [NODES_MAX]chan Packet
			for ind := 0; ind < len(myChilds); ind++ {
				chRcv[ind] = make(chan Packet)
				go waitForCommandWithTimeout(ln, 1000, chRcv[ind])
			}

			for ind := 0; ind < len(myChilds); ind++ {
				pck := <-chRcv[ind]
				if pck.Data > 0 { // 0 means that no connect was sent from this neightbour (timeout)
					acks = append(acks, byte(pck.Data))
				}
			}

			// If each child could ack, ack to parent
			if len(acks) == len(myChilds) && (all(acks, byte(Ack))) {
				sendToParent(node, myParent, Ack, 0)
			}
		}

	}
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
	time.Sleep(20 * time.Second) //Waiting all node to be ready
	//server("./nodes/node-1.yaml", true)
	time.Sleep(2 * time.Second) //Waiting all console return from nodes
}
