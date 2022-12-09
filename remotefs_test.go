package remotefs

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type remoteFSWithNetwork struct {
	*RemoteFS
	Conn io.ReadWriteCloser
}
type remoteFSWithNetworkSuite struct {
	suite.Suite
	server  *remoteFSWithNetwork
	client  *remoteFSWithNetwork
	workdir string
	Close   func() error
}

func (s *remoteFSWithNetworkSuite) SetupSuite() {
	workdir, err := os.MkdirTemp("", "dir-")
	if err != nil {
		log.Fatalf("Failed to create workdir: %v", err)
	}
	s.workdir = workdir
}

func (s *remoteFSWithNetworkSuite) TearDownTest() {
	s.Close()
}

func (s *remoteFSWithNetworkSuite) TearDownSuite() {
	os.RemoveAll(s.workdir)
}

func (s *remoteFSWithNetworkSuite) TestOpen() {
	require := require.New(s.T())

	fpath := filepath.Join(s.workdir, "testopen")
	rf, err := s.client.OpenFile(fpath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0755)
	require.Nil(err)
	require.NotNil(rf)

	_, err = os.Stat(fpath)
	require.Nil(err)
}

func (s *remoteFSWithNetworkSuite) TestClose() {
	require := require.New(s.T())

	fpath := filepath.Join(s.workdir, "testclose")
	fd := "0"

	f, err := os.OpenFile(fpath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0755)
	require.Nil(err)

	s.server.fdMap[fd] = &File{
		File:  f,
		Mutex: sync.Mutex{},
	}

	rf := &RemoteFile{
		fs: s.client,
		FD: fd,
	}
	err = rf.Close()
	require.Nil(err)

	s.server.mutex.Lock()
	defer s.server.mutex.Unlock()

	got, ok := s.server.fdMap[fd]
	require.False(ok)
	require.Nil(got)
}

func (s *remoteFSWithNetworkSuite) TestMkdir() {
	require := require.New(s.T())
	fpath := filepath.Join(s.workdir, "mkdir")

	err := s.client.Mkdir(fpath, 0755)
	require.Nil(err)

	info, err := os.Stat(fpath)
	require.Nil(err)
	require.True(info.IsDir())
}

func (s *remoteFSWithNetworkSuite) TestMkdirAll() {
	require := require.New(s.T())
	fpath := filepath.Join(s.workdir, "deep", "mkdir")

	err := s.client.MkdirAll(fpath, 0755)
	require.Nil(err)

	info, err := os.Stat(fpath)
	require.Nil(err)
	require.True(info.IsDir())
}

func (s *remoteFSWithNetworkSuite) TestRemove() {
	require := require.New(s.T())
	fpath := filepath.Join(s.workdir, "mkdir")
	err := os.MkdirAll(fpath, 0755)
	require.Nil(err)

	err = s.client.Remove(fpath)
	require.Nil(err)

	_, err = os.Stat(fpath)
	require.NotNil(err)
}

func (s *remoteFSWithNetworkSuite) TestRemoveAll() {
	require := require.New(s.T())
	fpath := filepath.Join(s.workdir, "deep", "mkdir")
	err := os.MkdirAll(fpath, 0755)
	require.Nil(err)

	subdirPath, err := os.MkdirTemp(fpath, "dir-")
	require.Nil(err)

	err = s.client.RemoveAll(fpath)
	require.Nil(err)

	_, err = os.Stat(subdirPath)
	require.NotNil(err)
}

func (s *remoteFSWithNetworkSuite) TestWrite() {
	require := require.New(s.T())

	fpath := filepath.Join(s.workdir, "testwrite")
	fd := "0"

	f, err := os.OpenFile(fpath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0755)
	require.Nil(err)

	s.server.fdMap[fd] = &File{
		File:  f,
		Mutex: sync.Mutex{},
	}

	rf := &RemoteFile{
		fs: s.client,
		FD: fd,
	}

	srcPath := filepath.Join("testdata", "pride-and-prejudice.txt")
	src, err := os.Open(srcPath)
	require.Nil(err)
	defer src.Close()

	_, err = io.Copy(rf, src)
	require.Nil(err)

	expected, _ := ioutil.ReadFile(srcPath)
	got, err := ioutil.ReadFile(fpath)
	require.Nil(err)
	log.Debugf("Length: %v == %v", len(expected), len(got))
	require.Equal(len(expected), len(got))
	require.Equal(string(expected), string(got))
}

func (s *remoteFSWithNetworkSuite) TestSeek() {
	require := require.New(s.T())

	srcPath := filepath.Join("testdata", "pride-and-prejudice.txt")
	fd := "0"

	f, err := os.OpenFile(srcPath, os.O_WRONLY, 0744)
	require.Nil(err)
	defer f.Close()

	s.server.fdMap[fd] = &File{
		File:  f,
		Mutex: sync.Mutex{},
	}

	rf := &RemoteFile{
		fs: s.client,
		FD: fd,
	}

	expected := int64(65535)
	ret, err := rf.Seek(expected, io.SeekStart)
	require.Nil(err)
	require.Equal(expected, ret)

	got, err := f.Seek(0, io.SeekCurrent)
	require.Nil(err)
	require.Equal(expected, got)
}

func (s *remoteFSWithNetworkSuite) TestRead() {
	require := require.New(s.T())

	fpath := filepath.Join("testdata", "pride-and-prejudice.txt")
	fd := "0"

	f, err := os.OpenFile(fpath, os.O_RDONLY, 0755)
	require.Nil(err)

	s.server.fdMap[fd] = &File{
		File:  f,
		Mutex: sync.Mutex{},
	}

	rf := &RemoteFile{
		fs: s.client,
		FD: fd,
	}

	got, err := io.ReadAll(rf)
	require.Nil(err)

	expected, _ := ioutil.ReadFile(fpath)
	log.Debugf("Length: %v == %v", len(expected), len(got))
	require.Equal(len(expected), len(got))
	require.Equal(string(expected), string(got))
}

func (s *remoteFSWithNetworkSuite) TestParallelRequests() {
	require := require.New(s.T())

	fpath := filepath.Join("testdata", "pride-and-prejudice.txt")

	numThreads := 5

	remoteFiles := make([]*RemoteFile, numThreads)
	for idx := 0; idx < numThreads; idx++ {
		f, err := os.OpenFile(fpath, os.O_RDONLY, 0755)
		require.Nil(err)

		fd := fmt.Sprintf("%v", idx)
		s.server.fdMap[fd] = &File{
			File:  f,
			Mutex: sync.Mutex{},
		}
		remoteFiles[idx] = &RemoteFile{
			fs: s.client,
			FD: fd,
		}
	}

	expected, _ := ioutil.ReadFile(fpath)

	wg := sync.WaitGroup{}

	// Setup goroutines that will parallelly write data on the connection
	// Start them together
	c := make(chan struct{})
	for idx := 0; idx < numThreads; idx++ {
		rf := remoteFiles[idx]
		wg.Add(1)
		go func(rf *RemoteFile) {
			defer wg.Done()
			<-c
			got, err := ioutil.ReadAll(rf)
			require.Nil(err)
			require.Equal(expected, got)
		}(rf)
	}

	for idx := 0; idx < numThreads; idx++ {
		c <- struct{}{}
	}
	wg.Wait()
}
