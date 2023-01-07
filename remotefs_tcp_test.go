package remotefs

import (
	"net"
	"sync"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
)

type remoteFSWithTCP struct {
	remoteFSWithNetworkSuite
}

func (s *remoteFSWithTCP) SetupTest() {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
	var c1 net.Conn
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		c1, _ = listener.Accept()
	}()

	c2, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		log.Fatalf("Failed to start client: %v", err)
	}

	wg.Wait()

	packetSize := 1024

	server, err := New("server", s.fs, createSendChan(s.T(), c1, "server"), createReceiveChan(s.T(), c1, "server"), packetSize)
	if err != nil {
		log.Fatalf("Failed to create remoteFS: %v", err)
	}
	server.Name = "server"
	s.server = &remoteFSWithNetwork{
		RemoteFS: server,
		Conn:     c1,
	}

	client, err := New("client", s.fs, createSendChan(s.T(), c2, "client"), createReceiveChan(s.T(), c2, "client"), packetSize)
	if err != nil {
		log.Fatalf("Failed to create remoteFS: %v", err)
	}
	client.Name = "client"
	s.client = &remoteFSWithNetwork{
		RemoteFS: client,
		Conn:     c2,
	}

	s.Close = func() error {
		server.Close()
		client.Close()
		c2.Close()
		c1.Close()
		listener.Close()
		return nil
	}
}

func TestRemoteFSWithTCP(t *testing.T) {
	// log.SetLevel(log.DebugLevel)
	suite.Run(t, new(remoteFSWithTCP))
}
