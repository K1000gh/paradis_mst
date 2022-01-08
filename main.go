package main

import (
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
	Test                = 3
	Accept              = 4
	Reject              = 5
	Merge               = 6
	Ack                 = 7
)

type Packet struct {
	Cmd  Command
	Data byte
	Src  byte
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

/*func send(nodeAddress string, neighAddress string) {
	myLog(nodeAddress, "Sending message to "+neighAddress)
	outConn, err := net.Dial("tcp", neighAddress+PORT)
	if err != nil {
		log.Fatal(err)
		return
	}
	outConn.Write([]byte(nodeAddress))
	outConn.Close()
}*/

func sendCommand(neighAddress string, cmd Command, data byte, src byte) {
	outConn, err := net.Dial("tcp", neighAddress+PORT)
	if err != nil {
		log.Fatal(err)
		return
	}

	outConn.Write([]byte{byte(cmd), data, src, DELIMITER})
	outConn.Close()
}

/*func waitForCommand(ln net.Listener) Packet {
	conn, _ := ln.Accept()
	rcvd, _ := bufio.NewReader(conn).ReadBytes(DELIMITER)
	conn.Close()

	var pck Packet
	pck.Cmd = Command(rcvd[0])
	pck.Data = rcvd[1]
	return pck
}*/

/*func sendToAllNeighbours(node yamlConfig, cmd Command, data byte) {
	myLog(node.Address, "Sending message to all neighbours...")
	for _, neigh := range node.Neighbours {
		go send(node.Address, neigh.Address)
	}
}*/

func sendToChilds(node yamlConfig, childs []byte, cmd Command, data byte) {
	for _, neigh := range node.Neighbours {
		if contains(childs, neigh.ID) {
			sendCommand(neigh.Address, cmd, data, node.ID)
		}
	}
}

func sendToNeighbours(node yamlConfig, cmd Command, data byte) {
	for _, neigh := range node.Neighbours {
		sendCommand(neigh.Address, cmd, data, node.ID)
	}
}

func sendToParent(node yamlConfig, parentId byte, cmd Command, data byte) {
	for _, neigh := range node.Neighbours {
		if neigh.ID == parentId {
			sendCommand(neigh.Address, cmd, data, node.ID)
		}
	}
}

func pollPacketsReceive(node yamlConfig, srv *Server) []Packet {
	var answerPcks []Packet
	for ind := 0; ind < 200; ind++ {
		time.Sleep(10 * time.Millisecond)
		answerPcks = srv.getAnswerPackets(node)

		if len(answerPcks) > 0 {
			break
		}
	}

	return answerPcks
}

func server(neighboursFilePath string) {
	// Load node config and start server
	var node yamlConfig = initAndParseFileNeighbours(neighboursFilePath)
	srv := NewServer(node.Address + PORT)

	// Wait for all nodes to initialize and send connection to neightbour with lowest weight
	time.Sleep(2000 * time.Millisecond)
	lowestNeight := getLowestWeightNeighbour(node)
	sendCommand(lowestNeight.Address, Connect, node.ID, node.ID)

	// Wait for the answer from neightbours
	time.Sleep(2000 * time.Millisecond)
	answerPcks := srv.getAnswerPackets(node)

	var connectRcvdIds []byte
	for _, pck := range answerPcks {
		connectRcvdIds = append(connectRcvdIds, pck.Data)
	}

	fragmentID := node.ID
	parentID := node.ID

	// Check if I'm the root of fragment
	if contains(connectRcvdIds, lowestNeight.ID) && (lowestNeight.ID > node.ID) {
		myChilds := connectRcvdIds // All Connect received are children for a root

		// Send NewFragment
		sendToChilds(node, connectRcvdIds, NewFragment, node.ID)
		myLog(node.Address, "I'm the fragment's root, sent NewFragment")

		// Wait for Ack to NewFragment from all children
		time.Sleep(2000 * time.Millisecond)
		childAnswerPcks := srv.getAnswerPackets(node)
		allChildsAck := true
		if len(childAnswerPcks) != len(myChilds) {
			allChildsAck = false
		} else {
			for ind := 0; ind < len(myChilds); ind++ {
				if childAnswerPcks[ind].Cmd != Ack {
					allChildsAck = false
				}
			}
		}

		if allChildsAck {
			myLog(node.Address, "All children Acknowledged to NewFragment")
		} else {
			myLog(node.Address, "ERROR: Not all children Acknowledged to NewFragment")
			for {
			} // Loop forever
		}

		// Send Test to detect lowest links on the border of the fragments

	} else {
		// Connected received from the root (double arrow) is not its child, to remove
		var myChilds []byte
		for ind := 0; ind < len(connectRcvdIds); ind++ {
			if connectRcvdIds[ind] != lowestNeight.ID {
				myChilds = append(myChilds, connectRcvdIds[ind])
			}
		}
		parentID = lowestNeight.ID

		// Wait to receive NewFragment from master
		answerPcks := pollPacketsReceive(node, srv)
		if (len(answerPcks) > 0) && (answerPcks[0].Cmd == NewFragment) {
			fragmentID = answerPcks[0].Data
			myLog(node.Address, "I'm a part of fragment ID "+string(fragmentID+'0'))

			// Send back new fragment to children, otherwise Acknowledge to parent that complete
			if len(myChilds) == 0 {
				sendToParent(node, parentID, Ack, 0)
				myLog(node.Address, "Send Ack to NewFragment to "+string(parentID+'0'))
			} else {
				sendToChilds(node, myChilds, NewFragment, fragmentID)

				// Wait for Ack from child
				childAnswerPcks := pollPacketsReceive(node, srv)
				if (len(childAnswerPcks) > 0) && (childAnswerPcks[0].Cmd == Ack) {
					myLog(node.Address, "Send Ack to NewFragment to "+string(parentID+'0'))
					sendToParent(node, parentID, Ack, 0)
				}
			}

		}
	}

	time.Sleep(2000 * time.Millisecond)

	/*for _, neigh := range node.Neighbours {
		sendCommand(neigh.Address, Test, fragmentID)
		answer := srv.getPacket()
	}*/
	sendToNeighbours(node, Test, fragmentID)
	time.Sleep(2000 * time.Millisecond)
	testPcks := srv.getAnswerPackets(node) // Recv pcks

	for _, pck := range testPcks {
		if fragmentID == pck.Data {
			for _, neigh := range node.Neighbours {
				if neigh.ID == pck.Src {
					sendCommand(neigh.Address, Reject, 0, node.ID)
				}
			}
		} else {
			for _, neigh := range node.Neighbours {
				if neigh.ID == pck.Src {
					sendCommand(neigh.Address, Accept, 0, node.ID)
				}
			}
		}
	}

	time.Sleep(2000 * time.Millisecond)

	var testAccept []byte
	for _, pck := range srv.getAnswerPackets(node) {
		if pck.Cmd == Accept {
			testAccept = append(testAccept, pck.Src)
		}
	}

	/*if len(testAccept) > 0 {
		for _, accept := testAccept {

		}
		sendToParent(node, parentID, Report, )
	}*/

	fmt.Printf("%v", testAccept)

	srv.Stop()
}

func main() {
	//localadress := "127.0.0.1"
	go server("./nodes/node-1.yaml")
	go server("./nodes/node-2.yaml")
	go server("./nodes/node-3.yaml")
	go server("./nodes/node-4.yaml")
	go server("./nodes/node-5.yaml")
	go server("./nodes/node-6.yaml")
	go server("./nodes/node-7.yaml")
	go server("./nodes/node-8.yaml")
	time.Sleep(20 * time.Second)
}
