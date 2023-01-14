package remotefs

import (
	"io"
	"io/fs"
	"os"

	log "github.com/sirupsen/logrus"
)

type RemoteFile struct {
	fs FileSystem
	FD IDType
}

type FileInterface interface {
	Name() string
	io.ReadWriteCloser
	io.Seeker
	Stat() (os.FileInfo, error)
}

func (f *RemoteFile) sendRequest(op FileSystemOperation, data interface{}, responseBufs ...[]byte) (*ParsedResponse, error) {
	req, err := createNewRequest(op, data)
	if err != nil {
		return nil, err
	}
	var buf []byte
	if len(responseBufs) > 0 {
		buf = responseBufs[0]
	}
	return f.fs.SendRequest(req, buf)
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
		fs: r,
	}, nil
}

func (f *RemoteFile) Close() error {
	closeReq := CloseRequest{
		FD: f.FD,
	}

	res, err := f.sendRequest(CloseOp, &closeReq)
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
	log.Debugf("Making read request of length: %v", len(b))
	res, err := f.sendRequest(ReadOp, &readReq, b)
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
	res, err := f.sendRequest(WriteOp, &writeReq)
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
	seekReq := SeekRequest{
		FD:     f.FD,
		Offset: offset,
		Whence: whence,
	}

	res, err := f.sendRequest(SeekOp, &seekReq)
	if err != nil {
		return 0, err
	}
	if res.Error != nil {
		return 0, res.Error
	}

	seekRes := res.Data.(*SeekResponse)
	return seekRes.Position, nil
}

func (f *RemoteFile) Chmod(mode os.FileMode) error {
	chmodReq := ChmodRequest{
		FD:   f.FD,
		Mode: mode,
	}
	res, err := f.sendRequest(ChmodOp, &chmodReq)
	if err != nil {
		return err
	}
	if res.Error != nil {
		return res.Error
	}
	return nil
}

func (f *RemoteFile) Chown(uid int, gid int) error {
	chownReq := ChownRequest{
		FD:  f.FD,
		UID: uid,
		GID: gid,
	}
	res, err := f.sendRequest(ChownOp, &chownReq)
	if err != nil {
		return err
	}
	if res.Error != nil {
		return res.Error
	}
	return nil
}

func (f *RemoteFile) Stat() (os.FileInfo, error) {
	statReq := StatRequest{
		FD: f.FD,
	}
	res, err := f.sendRequest(StatOp, &statReq)
	if err != nil {
		return nil, err
	}
	if res.Error != nil {
		return nil, res.Error
	}

	statRes := res.Data.(*StatResponse)
	return statRes.FileInfo, nil
}
