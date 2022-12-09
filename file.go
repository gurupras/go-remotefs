package remotefs

import (
	"io/fs"

	log "github.com/sirupsen/logrus"
)

type RemoteFile struct {
	fs FileSystem
	FD IDType
}

func (r *RemoteFS) OpenFile(path string, flags int, perm fs.FileMode) (*RemoteFile, error) {
	openReq := OpenRequest{
		Path:  path,
		Flags: flags,
		Perm:  perm,
	}
	req, err := createNewRequest(OpenOp, &openReq)
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

	openRes := res.Data.(*OpenResponse)
	return &RemoteFile{
		FD: openRes.FD,
	}, nil
}

func (f *RemoteFile) Close() error {
	cr := CloseRequest{
		FD: f.FD,
	}

	req, err := createNewRequest(CloseOp, &cr)
	if err != nil {
		return err
	}

	res, err := f.fs.SendRequest(req)
	if err != nil {
		return err
	}
	if res.Error != nil {
		return res.Error
	}
	return nil
}

func (f *RemoteFile) Read(b []byte) (int, error) {
	readReq := ReadRequest{
		FD:     f.FD,
		Length: len(b),
	}
	req, err := createNewRequest(ReadOp, &readReq)
	if err != nil {
		return 0, err
	}
	log.Debugf("Making read request of length: %v", len(b))
	res, err := f.fs.SendRequest(req, b)
	if err != nil {
		return 0, err
	}
	if res.Error != nil {
		return 0, res.Error
	}
	readRes := res.Data.(*ReadResponse)
	return readRes.Length, nil
}

func (f *RemoteFile) Write(b []byte) (int, error) {
	writeReq := WriteRequest{
		FD:    f.FD,
		Bytes: b,
	}
	req, err := createNewRequest(WriteOp, &writeReq)
	if err != nil {
		return 0, err
	}
	res, err := f.fs.SendRequest(req)
	if err != nil {
		return 0, err
	}
	if res.Error != nil {
		return 0, res.Error
	}
	writeRes := res.Data.(*WriteResponse)
	return writeRes.Length, nil
}

func (f *RemoteFile) Seek(offset int64, whence int) (int64, error) {
	seekReq := &SeekRequest{
		FD:     f.FD,
		Offset: offset,
		Whence: whence,
	}
	req, err := createNewRequest(SeekOp, &seekReq)
	if err != nil {
		return 0, err
	}
	res, err := f.fs.SendRequest(req)
	if err != nil {
		return 0, err
	}
	if res.Error != nil {
		return 0, res.Error
	}
	seekRes := res.Data.(*SeekResponse)
	return seekRes.Position, nil
}
