// Based on https://stackoverflow.com/questions/66755407/cancelling-a-net-listener-via-context-in-golang

package main

import (
	"io"
	"log"
	"net"
	"sync"
)

var DELIMITER byte = 255

type Server struct {
	listener   net.Listener
	quit       chan interface{}
	wg         sync.WaitGroup
	packetsCnt int
	chData     chan []byte
}

func NewServer(addr string) *Server {
	s := &Server{
		quit: make(chan interface{}),
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	s.listener = l
	s.wg.Add(1)
	s.packetsCnt = 0
	s.chData = make(chan []byte)
	go s.serve()
	return s
}

func (s *Server) Stop() {
	close(s.quit)
	s.listener.Close()
	s.wg.Wait()
}

func (s *Server) serve() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return
			default:
				log.Println("accept error", err)
			}
		} else {
			s.wg.Add(1)
			go func() {
				s.handleConection(conn)
				s.wg.Done()
			}()
		}
	}
}

func (s *Server) handleConection(conn net.Conn) {
	defer conn.Close()
	buf := make([]byte, 2048)
	for {
		//n, err := conn.Read(buf)
		n, err := conn.Read(buf)
		if err != nil && err != io.EOF {
			log.Println("read error", err)
			return
		}
		if n == 0 {
			return
		}
		s.packetsCnt++
		//log.Printf("received from %v: %s (%d)", conn.RemoteAddr(), string(buf[:n]), n)
		// fmt.Println(string(buf[:n]))
		s.chData <- buf[:n]
	}
}

func (s *Server) isPacketAvalible() bool {
	if s.packetsCnt > 0 {
		return true
	} else {
		return false
	}
}

func (s *Server) getPacket() Packet {
	data := <-s.chData

	var pck Packet
	pck.Cmd = Command(data[0])
	pck.Src = data[1]
	pck.Data = data[2:]
	s.packetsCnt--
	return pck
}

// Collect all received packets since last time called from all neighbours
func (s *Server) getAnswerPackets(node yamlConfig) []Packet {
	var packets []Packet
	for ind := 0; ind < len(node.Neighbours); ind++ {
		if s.isPacketAvalible() {
			packets = append(packets, s.getPacket())
		}
	}

	return packets
}
