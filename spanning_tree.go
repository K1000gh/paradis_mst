/*
	Project : Broadcast by waves demo for SDI course
	Author : Guillaume Riondet
	Date : July 2021
*/

package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"path/filepath"
	"strconv"
	"time"

	"gopkg.in/yaml.v2"
)

var PORT string = ":30000"

type yamlConfig struct {
	ID         int    `yaml:"id"`
	Address    string `yaml:"address"`
	Neighbours []struct {
		ID         int    `yaml:"id"`
		Address    string `yaml:"address"`
		EdgeWeight int    `yaml:"edge_weight"`
	} `yaml:"neighbours"`
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

func sendToAllNeighbours(node yamlConfig) {
	myLog(node.Address, "Sending message to all neighbours...")
	for _, neigh := range node.Neighbours {
		go send(node.Address, neigh.Address)
	}
}
func server(neighboursFilePath string, isStartingPoint bool) {
	var node yamlConfig = initAndParseFileNeighbours(neighboursFilePath)

	myLog(node.Address, "Starting server .... and listening ...")
	ln, err := net.Listen("tcp", node.Address+PORT)
	if err != nil {
		log.Fatal(err)
		return
	}
	var reach bool = false
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
	myLog(node.Address, "Message received from all neighboors, ending algorithm")
}

func main() {
	//localadress := "127.0.0.1"
	go server("./nodes/node-2.yaml", false)
	go server("./nodes/node-3.yaml", false)
	go server("./nodes/node-4.yaml", false)
	go server("./nodes/node-5.yaml", false)
	go server("./nodes/node-6.yaml", false)
	go server("./nodes/node-7.yaml", false)
	go server("./nodes/node-8.yaml", false)
	time.Sleep(2 * time.Second) //Waiting all node to be ready
	server("./nodes/node-1.yaml", true)
	time.Sleep(2 * time.Second) //Waiting all console return from nodes
}
