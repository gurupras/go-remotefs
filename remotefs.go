package remotefs

import (
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/vmihailenco/msgpack/v5"
)

var counter = uint64(0)

func generateID() IDType {
	newVal := atomic.AddUint64(&counter, 1)
	ret := IDType(fmt.Sprintf("%020d", newVal))
	return ret
}

func createNewRequest(reqType FileSystemOperation, data interface{}) (*Request, error) {
	id := generateID()
	b, err := msgpack.Marshal(data)
	if err != nil {
		return nil, err
	}
	req := &Request{
		ID:   id,
		Type: reqType,
		Data: b,
	}
	return req, nil
}

type File struct {
	FileInterface
	sync.Mutex
}

type Message struct {
	OpType
	data interface{}
}

type PartialMessage struct {
	id       IDType
	op       OpType
	data     *FragmentedBytesBuffer
	totalLen int
}

type RemoteFS struct {
	Name              string
	fs                afero.Fs
	maxPacketSize     int
	mutex             sync.Mutex
	fdMap             map[IDType]*File
	responseMap       map[IDType]chan *ParsedResponse
	responseBufferMap map[IDType][]byte
	partialMessages   map[IDType]*PartialMessage
	incomingMessages  chan *Message
	stopped           bool
	sendChan          chan<- []byte
	receiveChan       <-chan []byte
}

func New(name string, fs afero.Fs, sendChan chan<- []byte, receiveChan <-chan []byte, maxPacketSize int) (*RemoteFS, error) {
	if maxPacketSize != 0 && maxPacketSize < HeaderSize {
		return nil, fmt.Errorf("maxPacketSize (%v) must be %v", maxPacketSize, HeaderSize)
	}

	responseMap := make(map[IDType]chan *ParsedResponse)

	ret := &RemoteFS{
		Name:              name,
		fs:                fs,
		mutex:             sync.Mutex{},
		maxPacketSize:     maxPacketSize,
		fdMap:             make(map[IDType]*File),
		responseBufferMap: make(map[IDType][]byte),
		responseMap:       responseMap,
		partialMessages:   make(map[IDType]*PartialMessage),
		incomingMessages:  make(chan *Message),
		sendChan:          sendChan,
		receiveChan:       receiveChan,
	}
	go func() {
		err := ret.handleIncomingFragments()
		if err != nil {
			log.Errorf("[%v]: Error when handling incoming fragment: %v", ret.Name, err)
		}
	}()

	go func() {
		err := ret.handleIncomingMessages()
		if err != nil {
			log.Errorf("[%v]: Error when handling incoming message: %v", ret.Name, err)
		}
	}()
	return ret, nil
}

func (r *RemoteFS) Close() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	defer close(r.sendChan)
	r.stopped = true
}

func (r *RemoteFS) SendRequest(req *Request, buf ...[]byte) (*ParsedResponse, error) {
	id := req.ID
	r.mutex.Lock()
	respChan := make(chan *ParsedResponse)
	r.responseMap[id] = respChan
	if len(buf) > 0 {
		r.responseBufferMap[req.ID] = buf[0]
	}
	r.mutex.Unlock()

	err := r.encodeFragment(req.ID, RequestOp, req)
	if err != nil {
		return nil, err
	}
	log.Debugf("[%v]: Sent request id=%v type=%v", r.Name, req.ID, FileSystemOperationToString(req.Type))
	resp := <-respChan
	return resp, nil
}

func (r *RemoteFS) SendResponse(res *Response) error {
	err := r.encodeFragment(res.ID, ResponseOp, res)
	if err != nil {
		return err
	}
	log.Debugf("[%v]: Sent response type=%v", r.Name, FileSystemOperationToString(res.Type))
	return nil
}

func (r *RemoteFS) sendResponse(req *Request, data interface{}, err error) error {
	res := &Response{
		ID:   req.ID,
		Type: req.Type,
	}
	if err != nil {
		res.Error = err.Error()
	}
	if data != nil {
		b, err := msgpack.Marshal(data)
		if err != nil {
			return err
		}
		res.Data = b
	}
	return r.SendResponse(res)
}

func (r *RemoteFS) handleIncomingMessages() error {
	for msg := range r.incomingMessages {
		switch msg.OpType {
		case RequestOp:
			{
				req := msg.data.(*Request)
				log.Debugf("[%v]: Recevied request. type=%v", r.Name, FileSystemOperationToString(req.Type))
				err := r.handleIncomingRequest(req)
				if err != nil {
					log.Errorf("[%v]: Failed to handle incoming request: %v", r.Name, err)
					return err
				}
			}
		case ResponseOp:
			{
				res := msg.data.(*Response)
				log.Debugf("[%v]: Recevied response. type=%v", r.Name, FileSystemOperationToString(res.Type))
				err := r.handleIncomingResponse(res)
				if err != nil {
					log.Errorf("[%v]: Failed to handle incoming response: %v", r.Name, err)
					return err
				}
			}
		}
	}
	return nil
}

func (r *RemoteFS) handleIncomingFragments() error {
	isStopped := func() bool {
		r.mutex.Lock()
		defer r.mutex.Unlock()
		return r.stopped
	}
	for b := range r.receiveChan {
		if isStopped() {
			break
		}
		err := r.decodeFragment(b)
		if err != nil {
			stopped := isStopped()
			log.Debugf("[%v]: Encountered error and stopped=%v", r.Name, stopped)
			if isStopped() {
				return nil
			}
			return err
		}
	}
	return nil
}

func (r *RemoteFS) decodeFragment(b []byte) error {
	var frag Fragment
	err := msgpack.Unmarshal(b, &frag)
	if err != nil {
		return err
	}
	log.Debugf("[%v]: Decoded fragment id=%v size=%v end=%v type=%v", r.Name, frag.ID, len(frag.Data), frag.End, frag.OpType)

	partial, ok := r.partialMessages[frag.ID]
	if !ok {
		r.mutex.Lock()
		p := &PartialMessage{
			id:       frag.ID,
			op:       frag.OpType,
			data:     newFragmentedBytesBuffer(),
			totalLen: 0,
		}
		r.partialMessages[frag.ID] = p
		r.mutex.Unlock()
		partial = p
	}
	partial.data.Write(frag.Data)

	var data interface{}

	if frag.End {
		switch frag.OpType {
		case RequestOp:
			{
				var req Request
				data = &req
			}
		case ResponseOp:
			{
				var res Response
				data = &res
			}
		}
		decoder := msgpack.NewDecoder(partial.data)
		err := decoder.Decode(data)
		if err != nil {
			return err
		}
		msg := &Message{
			OpType: frag.OpType,
			data:   data,
		}
		r.incomingMessages <- msg
	}
	return nil
}

func (r *RemoteFS) encodeFragment(id IDType, op OpType, data interface{}) error {
	dataBytes, err := msgpack.Marshal(data)
	if err != nil {
		return err
	}

	maxDataSize := r.maxPacketSize - HeaderSize

	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.maxPacketSize == 0 || len(dataBytes) < maxDataSize-64 { // FIXME: We need to account for fragment size
		frag := Fragment{
			ID:     id,
			OpType: op,
			Data:   dataBytes,
			End:    true,
		}
		fragBytes, err := msgpack.Marshal(frag)
		if err != nil {
			return err
		}
		r.sendChan <- fragBytes
		log.Debugf("[%v]: Encoded fragment id=%v size=%v end=%v type=%v", r.Name, frag.ID, len(frag.Data), frag.End, frag.OpType)
	} else {
		sent := 0
		for {
			var frag Fragment
			frag.ID = id

			remaining := len(dataBytes) - sent
			chunkSize := maxDataSize - 64
			if remaining < chunkSize {
				chunkSize = remaining
				frag.OpType = op
				frag.End = true
			} else {
				frag.End = false
			}
			frag.Data = dataBytes[sent : sent+chunkSize]
			fragBytes, err := msgpack.Marshal(frag)
			if err != nil {
				return err
			}
			r.sendChan <- fragBytes
			log.Debugf("[%v]: Encoded fragment id=%v size=%v end=%v type=%v", r.Name, frag.ID, len(frag.Data), frag.End, frag.OpType)
			sent += chunkSize
			if sent == len(dataBytes) {
				break
			}
		}
	}
	return nil
}

func (r *RemoteFS) handleIncomingRequest(rawReq *Request) error {
	switch rawReq.Type {
	case OpenOp:
		{
			var req OpenRequest
			err := msgpack.Unmarshal(rawReq.Data, &req)
			if err != nil {
				return err
			}
			file, err := r.fs.OpenFile(req.Path, req.Flags, req.Perm)
			if err != nil {
				return r.sendResponse(rawReq, nil, err)
			}
			fd := generateID()
			var openRes *OpenResponse
			func() {
				r.mutex.Lock()
				defer r.mutex.Unlock()
				r.fdMap[fd] = &File{
					FileInterface: file,
					Mutex:         sync.Mutex{},
				}
				openRes = &OpenResponse{
					FD: fd,
				}
			}()
			return r.sendResponse(rawReq, openRes, nil)
		}
	case CloseOp:
		{
			var req CloseRequest
			err := msgpack.Unmarshal(rawReq.Data, &req)
			if err != nil {
				return err
			}
			func() {
				r.mutex.Lock()
				defer r.mutex.Unlock()
				if f, ok := r.fdMap[req.FD]; !ok {
					err = fmt.Errorf("bad fd")
					return
				} else {
					f.Lock()
					defer f.Unlock()
					err = f.Close()
				}
				delete(r.fdMap, req.FD)
			}()
			return r.sendResponse(rawReq, nil, err)
		}
	case ReadOp:
		{
			var req ReadRequest
			err := msgpack.Unmarshal(rawReq.Data, &req)
			if err != nil {
				return err
			}
			var readRes *ReadResponse
			func() {
				r.mutex.Lock()
				defer r.mutex.Unlock()
				if f, ok := r.fdMap[req.FD]; !ok {
					err = fmt.Errorf("bad fd")
					return
				} else {
					var n int
					readBytes := make([]byte, req.Length)
					f.Lock()
					defer f.Unlock()
					n, err = f.Read(readBytes)
					readRes = &ReadResponse{
						Length: n,
						Bytes:  readBytes,
					}
				}
			}()
			log.Debugf("[%v]: Attempting to send read response of length: %v", r.Name, readRes.Length)
			return r.sendResponse(rawReq, readRes, err)
		}
	case WriteOp:
		{
			var req WriteRequest
			err := msgpack.Unmarshal(rawReq.Data, &req)
			if err != nil {
				return err
			}
			var writeRes *WriteResponse
			func() {
				r.mutex.Lock()
				defer r.mutex.Unlock()
				if f, ok := r.fdMap[req.FD]; !ok {
					err = fmt.Errorf("bad fd")
					return
				} else {
					var n int
					f.Lock()
					defer f.Unlock()
					n, err = f.Write(req.Bytes)
					writeRes = &WriteResponse{
						Length: n,
					}
				}
			}()
			return r.sendResponse(rawReq, writeRes, err)
		}
	case SeekOp:
		{
			var req SeekRequest
			err := msgpack.Unmarshal(rawReq.Data, &req)
			if err != nil {
				return err
			}
			var seekRes *SeekResponse
			func() {
				r.mutex.Lock()
				defer r.mutex.Unlock()
				if f, ok := r.fdMap[req.FD]; !ok {
					err = fmt.Errorf("bad fd")
					return
				} else {
					var pos int64
					f.Lock()
					defer f.Unlock()
					pos, err = f.Seek(req.Offset, req.Whence)
					seekRes = &SeekResponse{
						Position: pos,
					}
				}
			}()
			return r.sendResponse(rawReq, seekRes, err)
		}
	case MkdirOp:
		{
			var req MkdirRequest
			err := msgpack.Unmarshal(rawReq.Data, &req)
			if err != nil {
				return err
			}
			if req.Recursive {
				err = r.fs.MkdirAll(req.Path, req.Perm)
			} else {
				err = r.fs.Mkdir(req.Path, req.Perm)
			}
			return r.sendResponse(rawReq, nil, err)
		}
	case RmdirOp:
		{
			var req RmdirRequest
			err := msgpack.Unmarshal(rawReq.Data, &req)
			if err != nil {
				return err
			}
			if req.Recursive {
				err = r.fs.RemoveAll(req.Path)
			} else {
				err = r.fs.Remove(req.Path)
			}
			return r.sendResponse(rawReq, nil, err)
		}
	case ChmodOp:
		{
			var req ChmodRequest
			err := msgpack.Unmarshal(rawReq.Data, &req)
			if err != nil {
				return err
			}
			func() {
				r.mutex.Lock()
				defer r.mutex.Unlock()
				if f, ok := r.fdMap[req.FD]; !ok {
					err = fmt.Errorf("bad fd")
					return
				} else {
					f.Lock()
					defer f.Unlock()
					err = r.fs.Chmod(f.Name(), req.Mode)
				}
			}()
			return r.sendResponse(rawReq, nil, err)
		}
	case ChownOp:
		{
			var req ChownRequest
			err := msgpack.Unmarshal(rawReq.Data, &req)
			if err != nil {
				return err
			}
			func() {
				r.mutex.Lock()
				defer r.mutex.Unlock()
				if f, ok := r.fdMap[req.FD]; !ok {
					err = fmt.Errorf("bad fd")
					return
				} else {
					f.Lock()
					defer f.Unlock()
					err = r.fs.Chown(f.Name(), req.UID, req.GID)
				}
			}()
			return r.sendResponse(rawReq, nil, err)
		}

	case StatOp:
		{
			var req StatRequest
			err := msgpack.Unmarshal(rawReq.Data, &req)
			if err != nil {
				return err
			}
			var statRes StatResponse
			func() {
				r.mutex.Lock()
				defer r.mutex.Unlock()
				if f, ok := r.fdMap[req.FD]; !ok {
					err = fmt.Errorf("bad fd")
					return
				} else {
					f.Lock()
					defer f.Unlock()
					var fileInfo os.FileInfo
					fileInfo, err = f.Stat()
					if err != nil {
						return
					}
					statRes.FileInfo = createFileInfo(fileInfo)
				}
			}()
			return r.sendResponse(rawReq, statRes, err)
		}
	case ReadDirOp:
		{
			var req ReadDirRequest
			err := msgpack.Unmarshal(rawReq.Data, &req)
			if err != nil {
				return err
			}
			var readDirRes ReadDirResponse
			func() {
				var f afero.File
				f, err = r.fs.Open(req.Path)
				if err != nil {
					return
				}
				defer f.Close()
				var children []os.FileInfo
				children, err = f.Readdir(-1) // TODO: We should probably do some streaming here
				if err != nil {
					return
				}

				readDirRes.Children = make([]*FileInfo, len(children))
				for idx, child := range children {

					readDirRes.Children[idx] = createFileInfo(child)
				}
			}()
			return r.sendResponse(rawReq, readDirRes, err)
		}
	}
	return nil
}

func (r *RemoteFS) handleIncomingResponse(rawRes *Response) error {
	parsedResponse := &ParsedResponse{
		ID:   rawRes.ID,
		Type: rawRes.Type,
	}
	if rawRes.Error != "" {
		if rawRes.Error == io.EOF.Error() {
			parsedResponse.Error = io.EOF
		} else if rawRes.Error == io.ErrShortBuffer.Error() {
			parsedResponse.Error = io.ErrShortBuffer
		} else if rawRes.Error == io.ErrShortWrite.Error() {
			parsedResponse.Error = io.ErrShortWrite
		} else {
			parsedResponse.Error = fmt.Errorf(rawRes.Error)
		}
	}

	var res interface{}
	switch rawRes.Type {
	case OpenOp:
		{
			res = &OpenResponse{}
		}
	case CloseOp:
		{
		}
	case ReadOp:
		{
			readRes := &ReadResponse{}
			func() {
				r.mutex.Lock()
				defer r.mutex.Unlock()
				if b, ok := r.responseBufferMap[rawRes.ID]; ok {
					readRes.Bytes = b
				}
				delete(r.responseBufferMap, rawRes.ID)
			}()
			res = readRes
		}
	case WriteOp:
		{
			res = &WriteResponse{}
		}
	case SeekOp:
		{
			res = &SeekResponse{}
		}
	case StatOp:
		{
			res = &StatResponse{}
		}
	case ReadDirOp:
		{
			res = &ReadDirResponse{}
		}
	}
	if res != nil {
		err := msgpack.Unmarshal(rawRes.Data, res)
		if err != nil {
			return err
		}
		parsedResponse.Data = res
	}
	r.mutex.Lock()
	defer r.mutex.Unlock()
	c, ok := r.responseMap[rawRes.ID]
	if !ok {
		log.Errorf("[%v]: Unexpected response received for unknown request with ID: %v", r.Name, rawRes.ID)
		return fmt.Errorf("no request with ID: %v", rawRes.ID)
	}
	c <- parsedResponse
	close(c)
	delete(r.responseMap, rawRes.ID)
	return nil
}

func (r *RemoteFS) MkdirAll(name string, perm os.FileMode) error {
	return r.mkdir(name, perm, true)
}

func (r *RemoteFS) Mkdir(name string, perm os.FileMode) error {
	return r.mkdir(name, perm, false)
}

func (r *RemoteFS) mkdir(name string, perm os.FileMode, recursive bool) error {
	mkdirReq := &MkdirRequest{
		Path:      name,
		Perm:      perm,
		Recursive: recursive,
	}
	req, err := createNewRequest(MkdirOp, mkdirReq)
	if err != nil {
		return err
	}

	res, err := r.SendRequest(req)
	if err != nil {
		return err
	}
	if res.Error != nil {
		return res.Error
	}
	return nil
}

func (r *RemoteFS) Remove(name string) error {
	return r.remove(name, false)
}

func (r *RemoteFS) RemoveAll(name string) error {
	return r.remove(name, true)
}

func (r *RemoteFS) remove(name string, recursive bool) error {
	removeReq := &RmdirRequest{
		Path:      name,
		Recursive: recursive,
	}
	req, err := createNewRequest(RmdirOp, removeReq)
	if err != nil {
		return err
	}

	res, err := r.SendRequest(req)
	if err != nil {
		return err
	}
	if res.Error != nil {
		return res.Error
	}
	return nil
}

func (r *RemoteFS) ReadDir(path string, count int) ([]os.FileInfo, error) {
	readDirReq := &ReadDirRequest{
		Path:  path,
		Count: count,
	}

	req, err := createNewRequest(ReadDirOp, readDirReq)
	if err != nil {
		return nil, err
	}

	res, err := r.SendRequest(req)
	if err != nil {
		return nil, err
	}
	if res.Error != nil {
		return nil, res.Error
	}

	readDirRes := res.Data.(*ReadDirResponse)
	fileInfos := make([]os.FileInfo, len(readDirRes.Children))
	for idx, entry := range readDirRes.Children {
		fileInfos[idx] = entry
	}
	return fileInfos, nil
}
