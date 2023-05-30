package remotefs

import (
	"os"
	"time"
)

type FileSystem interface {
	SendRequest(req *Request, buf ...[]byte) (*ParsedResponse, error)
	SendResponse(res *Response) error
}

type FileSystemOperation = uint8

const (
	_ FileSystemOperation = iota
	OpenOp
	NameOp
	ReadOp
	ReadAtOp
	WriteOp
	SeekOp
	CloseOp
	RenameOp
	MkdirOp
	RmdirOp
	ReadDirOp
	ChmodOp
	ChownOp
	StatOp
)

func FileSystemOperationToString(op FileSystemOperation) string {
	switch op {
	case OpenOp:
		return "open"
	case NameOp:
		return "name"
	case ReadOp:
		return "read"
	case ReadAtOp:
		return "readAt"
	case WriteOp:
		return "write"
	case SeekOp:
		return "seek"
	case CloseOp:
		return "close"
	case RenameOp:
		return "rename"
	case MkdirOp:
		return "mkdir"
	case RmdirOp:
		return "rmdir"
	case ReadDirOp:
		return "readdir"
	case ChmodOp:
		return "chmod"
	case ChownOp:
		return "chown"
	case StatOp:
		return "stat"
	default:
		return "unknown"
	}
}

type OpType = string

const HeaderSize = (2 + 20) + (3) + (1)

const (
	RequestOp  OpType = "req"
	ResponseOp OpType = "res"
)

type IDType = string

type Response struct {
	ID    IDType              `json:"id" msgpack:"id"`
	Type  FileSystemOperation `json:"type" msgpack:"type"`
	OK    bool                `json:"ok" msgpack:"ok"`
	Error string              `json:"error" msgpack:"error"`
	Data  []byte              `json:"data" msgpack:"data"`
}

type ParsedResponse struct {
	ID    IDType              `json:"id" msgpack:"id"`
	Type  FileSystemOperation `json:"type" msgpack:"type"`
	OK    bool                `json:"ok" msgpack:"ok"`
	Error error               `json:"error" msgpack:"error"`
	Data  interface{}         `json:"data" msgpack:"data"`
}

type Request struct {
	ID   IDType              `json:"id" msgpack:"id"`
	Type FileSystemOperation `json:"type" msgpack:"type"`
	Data []byte              `json:"data" msgpack:"data"`
}

type OpenRequest struct {
	Path  string      `json:"path" msgpack:"path"`
	Flags []string    `json:"flags" msgpack:"flags"`
	Perm  os.FileMode `json:"perm" msgpack:"perm"`
}

type FileOperation struct {
	FD IDType `json:"fd" msgpack:"fd"`
}

type OpenResponse struct {
	FD IDType `json:"fd" msgpack:"fd"`
}

type NameRequest struct {
	FD IDType `json:"fd" msgpack:"fd"`
}

type NameResponse struct {
	Name string `json:"name" msgpack:"name"`
}

type ReadRequest struct {
	FD     IDType `json:"fd" msgpack:"fd"`
	Length int    `json:"length" msgpack:"length"`
}

type ReadAtRequest struct {
	ReadRequest
	Offset int64 `json:"offset" msgpack:"offset"`
}

type ReadResponse struct {
	Length int    `json:"length" msgpack:"length"`
	Bytes  []byte `json:"bytes" msgpack:"bytes"`
}

type WriteRequest struct {
	FD     IDType `json:"fd" msgpack:"fd"`
	Bytes  []byte `json:"bytes" msgpack:"bytes"`
	Offset int64  `json:"offset" msgpack:"offset"`
}

type WriteResponse struct {
	Length int `json:"length" msgpack:"length"`
}

type CloseRequest struct {
	FD IDType `json:"fd" msgpack:"fd"`
}

type SeekRequest struct {
	FD     IDType `json:"fd" msgpack:"fd"`
	Offset int64  `json:"offset" msgpack:"offset"`
	Whence int    `json:"whence" msgpack:"whence"`
}

type SeekResponse struct {
	Position int64 `json:"pos" msgpack:"pos"`
}

type RenameRequest struct {
	Old string `json:"old" msgpack:"old"`
	New string `json:"new" msgpack:"new"`
}

type MkdirRequest struct {
	Path      string      `json:"path" msgpack:"path"`
	Perm      os.FileMode `json:"perm" msgpack:"perm"`
	Recursive bool        `json:"recursive" msgpack:"recursive"`
}

type RmdirRequest struct {
	Path      string `json:"path" msgpack:"path"`
	Recursive bool   `json:"recursive" msgpack:"recursive"`
}

type ReadDirRequest struct {
	Path  string `json:"path" msgpack:"path"`
	Count int    `json:"count" msgpack:"count"`
}

type ReadDirResponse struct {
	Children []*FileInfo `json:"children" msgpack:"children"`
}

type ChmodRequest struct {
	FD   IDType      `json:"fd" msgpack:"fd"`
	Mode os.FileMode `json:"mode" msgpack:"mode"`
}

type ChownRequest struct {
	FD  IDType `json:"fd" msgpack:"fd"`
	UID int    `json:"uid" msgpack:"uid"`
	GID int    `json:"gid" msgpack:"gid"`
}

type StatRequest struct {
	FD IDType `json:"fd" msgpack:"fd"`
}

type FileInfo struct {
	FName    string      `json:"name" msgpack:"name"`
	FSize    int64       `json:"size" msgpack:"size"`
	FMode    os.FileMode `json:"mode" msgpack:"mode"`
	FModTime time.Time   `json:"modTime" msgpack:"modTime"`
	FIsDir   bool        `json:"isDir" msgpack:"isDir"`
	FSys     any         `json:"sys" msgpack:"sys"`
}

func (f *FileInfo) Name() string {
	return f.FName
}

func (f *FileInfo) Size() int64 {
	return f.FSize
}

func (f *FileInfo) Mode() os.FileMode {
	return f.FMode
}

func (f *FileInfo) ModTime() time.Time {
	return f.FModTime
}

func (f *FileInfo) IsDir() bool {
	return f.FIsDir
}

func (f *FileInfo) Sys() any {
	return f.FSys
}

func createFileInfo(fileInfo os.FileInfo) *FileInfo {
	ret := &FileInfo{
		FName:    fileInfo.Name(),
		FSize:    fileInfo.Size(),
		FMode:    fileInfo.Mode(),
		FModTime: fileInfo.ModTime(),
		FIsDir:   fileInfo.IsDir(),
		FSys:     nil, // TODO: Do we need this?
	}
	return ret
}

type StatResponse struct {
	*FileInfo
}
