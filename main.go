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

const PORT = ":30000"
const LOG = true
const CMDLOG = true

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

var COMMAND = []string{
	"CONNECT",
	"NEW_FRAG",
	"REPORT",
	"TEST",
	"ACCEPT",
	"REJECT",
	"MERGE",
}

type Packet struct {
	Cmd  Command
	Src  byte
	Data []byte
}

func getLowestWeightNeighbour(node yamlConfig) Neigh {
	var neighbours = node.Neighbours
	sort.Slice(neighbours, func(i, j int) bool {
		return neighbours[i].EdgeWeight < neighbours[j].EdgeWeight
	})

	return neighbours[0]
}

func getNeighbour(node yamlConfig, id byte) Neigh {
	for _, neigh := range node.Neighbours {
		if neigh.ID == id {
			return neigh
		}
	}

	return Neigh{}
}

func contains(s []byte, val byte) bool {
	for _, v := range s {
		if v == val {
			return true
		}
	}
	return false
}

func find(s []byte, val byte) int {
	for i, v := range s {
		if v == val {
			return i
		}
	}
	return len(s)
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
	if LOG {
		fmt.Printf("+ [%s] : %s\n", localAdress, message)
	}
}

func sendCommand(neighAddress string, cmd Command, data []byte, src byte) {
	outConn, err := net.Dial("tcp", neighAddress+PORT)
	if err != nil {
		log.Fatal(err)
		return
	}

	if CMDLOG {
		fmt.Printf(" { %s%v } From %s to %s\n", COMMAND[cmd], data, string(src+'0'), neighAddress)
	}

	outConn.Write(append([]byte{byte(cmd), src}, data[:]...))
	outConn.Close()
}

func sendToChilds(node yamlConfig, childs []byte, cmd Command, data []byte) {
	for _, neigh := range node.Neighbours {
		if contains(childs, neigh.ID) {
			sendCommand(neigh.Address, cmd, data, node.ID)
		}
	}
}

func sendToNeighbours(node yamlConfig, cmd Command, data []byte) {
	for _, neigh := range node.Neighbours {
		sendCommand(neigh.Address, cmd, data, node.ID)
	}
}

func sendToParent(node yamlConfig, parentId byte, cmd Command, data []byte) {
	for _, neigh := range node.Neighbours {
		if neigh.ID == parentId {
			sendCommand(neigh.Address, cmd, data, node.ID)
		}
	}
}

func pollPacketsReceive(node yamlConfig, srv *Server, n int) []Packet {
	var answerPcks []Packet
	for {
		time.Sleep(10 * time.Millisecond)
		answerPcks = append(answerPcks, srv.getAnswerPackets(node)[:]...)

		if len(answerPcks) >= n {
			break
		}
	}

	return answerPcks
}

func server(neighboursFilePath string) {
	// Load node config and start server
	var node = initAndParseFileNeighbours(neighboursFilePath)
	srv := NewServer(node.Address + PORT)

	// Wait for all nodes to initialize and send connection to neighbour with the lowest weight
	time.Sleep(1000 * time.Millisecond)
	lowestNeigh := getLowestWeightNeighbour(node)
	sendCommand(lowestNeigh.Address, Connect, []byte{node.ID}, node.ID)

	// Wait for the answer from neighbours
	time.Sleep(1000 * time.Millisecond)
	var connected []byte
	for _, pck := range srv.getAnswerPackets(node) {
		connected = append(connected, pck.Data[0])
	}

	fragmentID := node.ID
	parentID := node.ID
	root := contains(connected, lowestNeigh.ID) && (lowestNeigh.ID > node.ID)
	childs := connected

	for {
		if root {
			myLog(node.Address, "I'm the fragment's root")
		} else {
			// Wait to receive NEW_FRAG message from the root node
			newfrag := pollPacketsReceive(node, srv, 1)[0]

			fragmentID = newfrag.Data[0]
			parentID = newfrag.Src // New fragment is always received from the parent node
			myLog(node.Address, "I'm a part of fragment ID "+string(fragmentID+'0'))

			// Remove parent node from the children
			t := find(childs, parentID)
			if t < len(childs) {
				childs = append(childs[:t], childs[t+1:]...)
			}
		}

		sendToChilds(node, childs, NewFragment, []byte{fragmentID})
		time.Sleep(1000 * time.Millisecond)

		sendToNeighbours(node, Test, []byte{fragmentID})
		time.Sleep(1000 * time.Millisecond)

		for _, pck := range pollPacketsReceive(node, srv, len(node.Neighbours)) {
			neigh := getNeighbour(node, pck.Src)
			cmd := func() Command {
				if fragmentID == pck.Data[0] {
					return Reject
				} else {
					return Accept
				}
			}()

			sendCommand(neigh.Address, cmd, []byte{}, node.ID)
		}

		time.Sleep(1000 * time.Millisecond)

		var testAccept [][]byte
		for _, pck := range pollPacketsReceive(node, srv, len(node.Neighbours)) {
			if pck.Cmd == Accept {
				neigh := getNeighbour(node, pck.Src)
				testAccept = append(testAccept, []byte{neigh.ID, byte(neigh.EdgeWeight)})
			}
		}

		time.Sleep(1000 * time.Millisecond)

		// Nodes without any children starts to report
		if len(childs) > 0 {
			for _, pck := range pollPacketsReceive(node, srv, len(childs)) {
				if len(pck.Data) > 1 {
					testAccept = append(testAccept, []byte{pck.Data[0], pck.Data[1]})
				}
			}
		}

		sort.Slice(testAccept, func(i, j int) bool {
			return testAccept[i][1] < testAccept[j][1]
		})

		merge := func() Packet {
			if root {
				return Packet{Merge, node.ID, func() []byte {
					if len(testAccept) > 0 {
						return []byte{testAccept[0][0]}
					}

					return []byte{}
				}()}
			} else {
				sendToParent(node, parentID, Report, func() []byte {
					if len(testAccept) > 0 {
						return testAccept[0]
					}

					return []byte{}
				}())

				time.Sleep(1000 * time.Millisecond)
				return pollPacketsReceive(node, srv, 1)[0]
			}
		}()

		sendToChilds(node, childs, merge.Cmd, merge.Data)
		time.Sleep(1000 * time.Millisecond)

		if len(merge.Data) > 0 {
			// MERGE phase, reset root node
			root = false
			neigh := getNeighbour(node, merge.Data[0])

			if neigh != (Neigh{}) {
				myLog(node.Address, "I'm the neighbour in charge of the merge")

				sendCommand(neigh.Address, Connect, []byte{node.ID}, node.ID)
				rcv := pollPacketsReceive(node, srv, 1)[0]

				// The node with the lowest ID becomes the new root
				if node.ID < rcv.Data[0] {
					fragmentID = node.ID
					childs = append(childs, neigh.ID)
					childs = append(childs, parentID)
					parentID = node.ID
					root = true
				} else {
					parentID = neigh.ID
				}
			}
		} else {
			// FINISHED
			myLog(node.Address, "FINISHED")
			break
		}
	} // Back to step 2 (NEW_FRAG)

	srv.Stop()
}

func main() {
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
