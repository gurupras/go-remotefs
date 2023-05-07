package remotefs

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/vmihailenco/msgpack/v5"
)

type remoteFSWithNetwork struct {
	*RemoteFS
	Conn io.ReadWriteCloser
}
type remoteFSWithNetworkSuite struct {
	suite.Suite
	fs      *MockFS
	server  *remoteFSWithNetwork
	client  *remoteFSWithNetwork
	workdir string
	Close   func() error
}

type MockFS struct {
	mock.Mock
	afero.Fs
}

func (m *MockFS) Chmod(path string, mode os.FileMode) error {
	args := m.Called(path, os.FileMode(mode))
	return args.Error(0)
}

func (m *MockFS) Chown(path string, uid int, gid int) error {
	args := m.Called(path, uid, gid)
	return args.Error(0)
}

type MockFile struct {
	mock.Mock
	afero.File
}

type packetWrapper struct {
	Data []byte
}

func createSendChan(t *testing.T, writer io.Writer, name string) chan<- []byte {
	require := require.New(t)
	encoder := msgpack.NewEncoder(writer)
	ch := make(chan []byte)
	go func() {
		for b := range ch {
			pkt := &packetWrapper{b}
			if len(b) > 65535 {
				log.Warnf("[%v]: Attemping to send large packet", name)
			}
			err := encoder.Encode(pkt)
			require.Nil(err)
		}
	}()
	return ch
}

func createReceiveChan(t *testing.T, reader io.Reader, name string) <-chan []byte {
	require := require.New(t)
	_ = require
	decoder := msgpack.NewDecoder(reader)
	ch := make(chan []byte)
	go func() {
		defer close(ch)
		for {
			var pkt packetWrapper
			err := decoder.Decode(&pkt)
			if err != nil {
				log.Debugf("[%v]: Failed to decode packet: %v", name, err)
				break
			}
			ch <- pkt.Data
		}
	}()
	return ch
}

func (m *MockFile) Stat() (os.FileInfo, error) {
	args := m.Called()
	return args.Get(0).(os.FileInfo), args.Error(1)
}

func (s *remoteFSWithNetworkSuite) SetupSuite() {
	s.fs = &MockFS{
		Fs: afero.NewMemMapFs(),
	}
	err := s.fs.Mkdir("workdir", 0775)
	if err != nil {
		log.Fatalf("Failed to create workdir: %v", err)
	}
	s.workdir = "workdir"
}

func (s *remoteFSWithNetworkSuite) TearDownTest() {
	s.Close()
}

func (s *remoteFSWithNetworkSuite) TearDownSuite() {
	s.fs.RemoveAll(s.workdir)
}

func (s *remoteFSWithNetworkSuite) TestOpen() {
	require := require.New(s.T())

	fpath := filepath.Join(s.workdir, "testopen")
	rf, err := s.client.OpenFile(fpath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0755)
	require.Nil(err)
	require.NotNil(rf)

	_, err = s.fs.Stat(fpath)
	require.Nil(err)
}

func (s *remoteFSWithNetworkSuite) TestClose() {
	require := require.New(s.T())

	fpath := filepath.Join(s.workdir, "testclose")
	fd := "0"

	f, err := s.fs.OpenFile(fpath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0755)
	require.Nil(err)

	s.server.fdMap[fd] = &File{
		FileInterface: f,
		Mutex:         sync.Mutex{},
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

func (s *remoteFSWithNetworkSuite) TestRename() {
	require := require.New(s.T())
	oldPath := filepath.Join(s.workdir, "old")
	newPath := filepath.Join(s.workdir, "new")
	f, err := s.fs.OpenFile(oldPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0755)
	require.Nil(err)
	f.Close()

	err = s.client.Rename(oldPath, newPath)
	require.Nil(err)

	f, err = s.fs.Open(newPath)
	require.Nil(err)
	_, err = f.Stat()
	require.Nil(err)
}

func (s *remoteFSWithNetworkSuite) TestMkdir() {
	require := require.New(s.T())
	fpath := filepath.Join(s.workdir, "mkdir")

	err := s.client.Mkdir(fpath, 0755)
	require.Nil(err)

	info, err := s.fs.Stat(fpath)
	require.Nil(err)
	require.True(info.IsDir())
}

func (s *remoteFSWithNetworkSuite) TestMkdirAll() {
	require := require.New(s.T())
	fpath := filepath.Join(s.workdir, "deep", "mkdir")

	err := s.client.MkdirAll(fpath, 0755)
	require.Nil(err)

	info, err := s.fs.Stat(fpath)
	require.Nil(err)
	require.True(info.IsDir())
}

func (s *remoteFSWithNetworkSuite) TestRemove() {
	require := require.New(s.T())
	fpath := filepath.Join(s.workdir, "mkdir")
	err := s.fs.MkdirAll(fpath, 0755)
	require.Nil(err)

	err = s.client.Remove(fpath)
	require.Nil(err)

	_, err = s.fs.Stat(fpath)
	require.NotNil(err)
}

func (s *remoteFSWithNetworkSuite) TestRemoveAll() {
	require := require.New(s.T())
	fpath := filepath.Join(s.workdir, "deep", "mkdir")
	err := s.fs.MkdirAll(fpath, 0755)
	require.Nil(err)

	subdirPath := filepath.Join(fpath, "dir-01")
	err = s.fs.Mkdir(subdirPath, 0775)
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

	f, err := s.fs.OpenFile(fpath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0755)
	require.Nil(err)

	s.server.fdMap[fd] = &File{
		FileInterface: f,
		Mutex:         sync.Mutex{},
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

	var got []byte
	{
		f, err := s.fs.Open(fpath)
		require.Nil(err)
		defer f.Close()
		got, err = io.ReadAll(f)
		require.Nil(err)
	}
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
		FileInterface: f,
		Mutex:         sync.Mutex{},
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
		FileInterface: f,
		Mutex:         sync.Mutex{},
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
			FileInterface: f,
			Mutex:         sync.Mutex{},
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

func (s *remoteFSWithNetworkSuite) TestChmod() {
	require := require.New(s.T())

	fd := "0"

	fpath := filepath.Join(s.workdir, "testchmod")

	f, err := s.fs.Create(fpath)
	require.Nil(err)
	defer f.Close()

	mockFile := &MockFile{}
	mockFile.File = f

	s.fs.On("Chmod", mock.Anything, mock.Anything).Return(nil)

	s.server.fdMap[fd] = &File{
		FileInterface: mockFile,
		Mutex:         sync.Mutex{},
	}

	rf := &RemoteFile{
		fs: s.client,
		FD: fd,
	}

	expected := os.FileMode(0333)
	err = rf.Chmod(expected)
	require.Nil(err)

	// Expect function to have been called
	s.fs.AssertExpectations(s.T())

	s.fs.AssertCalled(s.T(), "Chmod", f.Name(), expected)
}

func (s *remoteFSWithNetworkSuite) TestChown() {
	// log.SetLevel(log.DebugLevel)
	require := require.New(s.T())

	fd := "0"

	fpath := filepath.Join(s.workdir, "testchmod")

	f, err := s.fs.Create(fpath)
	require.Nil(err)
	defer f.Close()

	mockFile := &MockFile{}
	mockFile.File = f

	s.fs.On("Chown", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.server.fdMap[fd] = &File{
		FileInterface: mockFile,
		Mutex:         sync.Mutex{},
	}

	rf := &RemoteFile{
		fs: s.client,
		FD: fd,
	}

	err = rf.Chown(0, 0)
	require.Nil(err)

	// Expect function to have been called
	s.fs.AssertExpectations(s.T())

	s.fs.AssertCalled(s.T(), "Chown", f.Name(), 0, 0)
}

func (s *remoteFSWithNetworkSuite) TestStat() {
	// log.SetLevel(log.DebugLevel)
	require := require.New(s.T())

	fpath := filepath.Join("testdata", "pride-and-prejudice.txt")
	fd := "0"

	f, err := os.OpenFile(fpath, os.O_RDONLY, 0755)
	require.Nil(err)
	defer f.Close()

	expected, err := f.Stat()
	require.Nil(err)

	mockFile := &MockFile{}
	mockFile.File = f
	mockFile.On("Stat").Return(expected, nil)

	s.server.fdMap[fd] = &File{
		FileInterface: mockFile,
		Mutex:         sync.Mutex{},
	}

	rf := &RemoteFile{
		fs: s.client,
		FD: fd,
	}

	got, err := rf.Stat()
	require.Nil(err)

	// Expect function to have been called
	mockFile.AssertExpectations(s.T())

	require.Equal(expected.Name(), got.Name())
	require.Equal(expected.Size(), got.Size())
	require.Equal(expected.Mode(), got.Mode())
	require.Equal(expected.ModTime(), got.ModTime())
	require.Equal(expected.IsDir(), got.IsDir())
}

func (s *remoteFSWithNetworkSuite) TestReadDir() {
	// log.SetLevel(log.DebugLevel)
	require := require.New(s.T())

	for idx := 0; idx < 10; idx++ {
		name := fmt.Sprintf("child-%03d", idx)
		fpath := filepath.Join(s.workdir, name)
		if idx%2 == 0 {
			// Create a file
			s.fs.Create(fpath)
		} else {
			// Create a dir
			err := s.fs.Mkdir(fpath, 0775)
			require.Nil(err)
		}
	}

	f, err := s.fs.Open(s.workdir)
	require.Nil(err)
	defer f.Close()

	expectedEntries, err := f.Readdir(-1)
	require.Nil(err)

	fd := "0"

	s.server.fdMap[fd] = &File{
		FileInterface: f,
		Mutex:         sync.Mutex{},
	}

	gotEntries, err := s.client.ReadDir(s.workdir, -1)
	require.Nil(err)

	require.Equal(len(expectedEntries), len(gotEntries))
	for idx := range expectedEntries {
		expected := expectedEntries[idx]
		got := gotEntries[idx]

		require.Equal(expected.Name(), got.Name())
		require.Equal(expected.Size(), got.Size())
		require.Equal(expected.Mode(), got.Mode())
		require.WithinDuration(expected.ModTime(), got.ModTime(), 10*time.Millisecond)
		require.Equal(expected.IsDir(), got.IsDir())
	}
}
