package remotefs

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pion/datachannel"
	"github.com/pion/webrtc/v3"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type remoteFSWithP2P struct {
	remoteFSWithNetworkSuite
}

type DCWithFlowControl struct {
	name              string
	dc                *webrtc.DataChannel
	raw               datachannel.ReadWriteCloser
	ch                chan struct{}
	maxBufferedAmount uint64
}

func newDCWithFlowControl(name string, dc *webrtc.DataChannel, raw datachannel.ReadWriteCloser, maxBufferedAmount uint64) *DCWithFlowControl {
	ret := &DCWithFlowControl{
		name:              name,
		dc:                dc,
		raw:               raw,
		ch:                make(chan struct{}),
		maxBufferedAmount: maxBufferedAmount,
	}
	dc.SetBufferedAmountLowThreshold(maxBufferedAmount / 2)
	counter := uint64(0)
	dc.OnBufferedAmountLow(func() {
		log.Debugf("[%v]: Buffered amount low", name)
		go func() {
			newVal := atomic.AddUint64(&counter, 1)
			log.Debugf("[%v]: Attempting to notify writable (counter=%v)", name, newVal)
			ret.ch <- struct{}{}
			log.Debugf("[%v]: Notified writable (counter=%v)", name, newVal)
		}()
	})
	return ret
}

func (dc *DCWithFlowControl) Read(b []byte) (int, error) {
	return dc.raw.Read(b)
}

func (dc *DCWithFlowControl) Write(b []byte) (int, error) {
	if dc.dc.BufferedAmount()+uint64(len(b)) > dc.maxBufferedAmount {
		log.Debugf("[%v]: Waiting for drain", dc.name)
		<-dc.ch
		log.Debugf("[%v]: Finished waiting for drain", dc.name)
	}
	return dc.raw.Write(b)
}

func (s *remoteFSWithP2P) SetupTest() {
	require := require.New(s.T())

	webrtcSettings := webrtc.SettingEngine{}
	webrtcSettings.DetachDataChannels()

	config := webrtc.Configuration{}

	ordered := true
	maxRetransmits := uint16(0)

	dcWG := sync.WaitGroup{}
	dcWG.Add(2)

	connectedWG := sync.WaitGroup{}
	connectedWG.Add(2)

	api := webrtc.NewAPI(webrtc.WithSettingEngine(webrtcSettings))

	pc1, err := api.NewPeerConnection(config)
	require.Nil(err)

	pc1.OnConnectionStateChange(func(pcs webrtc.PeerConnectionState) {
		log.Debugf("[pc1]: State changed: %v", pcs.String())
	})

	var rawDC1 datachannel.ReadWriteCloser
	dc1, err := pc1.CreateDataChannel("test", &webrtc.DataChannelInit{
		Ordered:        &ordered,
		MaxRetransmits: &maxRetransmits,
	})
	require.Nil(err)

	mutex := sync.Mutex{}

	dc1.OnOpen(func() {
		defer dcWG.Done()
		mutex.Lock()
		defer mutex.Unlock()
		rawDC1, err = dc1.Detach()
		require.Nil(err)
	})

	offer, err := pc1.CreateOffer(nil)
	require.Nil(err)

	offerGatheringComplete := webrtc.GatheringCompletePromise(pc1)

	err = pc1.SetLocalDescription(offer)
	require.Nil(err)

	<-offerGatheringComplete

	pc2, err := api.NewPeerConnection(config)
	require.Nil(err)

	pc2.OnConnectionStateChange(func(pcs webrtc.PeerConnectionState) {
		log.Debugf("[pc2]: State changed: %v", pcs.String())
	})

	var dc2 *webrtc.DataChannel
	var rawDC2 datachannel.ReadWriteCloser
	pc2.OnDataChannel(func(dc *webrtc.DataChannel) {
		dc.OnOpen(func() {
			defer dcWG.Done()
			mutex.Lock()
			defer mutex.Unlock()
			dc2 = dc
			rawDC2, err = dc.Detach()
			require.Nil(err)
		})
	})

	err = pc2.SetRemoteDescription(offer)
	require.Nil(err)

	answer, err := pc2.CreateAnswer(nil)
	require.Nil(err)

	answerGatheringComplete := webrtc.GatheringCompletePromise(pc2)

	err = pc2.SetLocalDescription(answer)
	require.Nil(err)

	<-answerGatheringComplete

	func() {
		mutex.Lock()
		defer mutex.Unlock()
		err = pc1.SetRemoteDescription(*pc2.LocalDescription())
		require.Nil(err)
	}()

	dcWG.Wait()
	// time.Sleep(time.Second)

	maxPacketSize := 65535
	c1 := newDCWithFlowControl("server", dc1, rawDC1, 1*1024*1024)
	c2 := newDCWithFlowControl("client", dc2, rawDC2, 1*1024*1024)

	server, err := New("server", s.fs, c1, c1, maxPacketSize)
	require.Nil(err)

	client, err := New("client", s.fs, c2, c2, maxPacketSize)
	require.Nil(err)

	s.server = &remoteFSWithNetwork{
		RemoteFS: server,
		Conn:     rawDC1,
	}

	s.client = &remoteFSWithNetwork{
		RemoteFS: client,
		Conn:     rawDC2,
	}

	s.Close = func() error {
		server.Close()
		client.Close()
		rawDC2.Close()
		rawDC1.Close()
		pc1.Close()
		pc2.Close()
		return nil
	}
}

func TestRemoteFSWithP2P(t *testing.T) {
	// log.SetLevel(log.DebugLevel)
	suite.Run(t, new(remoteFSWithP2P))
}
