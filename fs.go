package remotefs

import (
	"os"
)

type FileSystem interface {
	SendRequest(req *Request, buf ...[]byte) (*ParsedResponse, error)
	SendResponse(res *Response) error
}

type FileSystemOperation = uint8

const (
	_ FileSystemOperation = iota
	OpenOp
	ReadOp
	ReadAtOp
	WriteOp
	SeekOp
	CloseOp
	MkdirOp
	RmdirOp
	ReadDirOp
)

func FileSystemOperationToString(op FileSystemOperation) string {
	switch op {
	case OpenOp:
		return "open"
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
	case MkdirOp:
		return "mkdir"
	case RmdirOp:
		return "rmdir"
	case ReadDirOp:
		return "readdir"
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

type Fragment struct {
	ID IDType
	OpType
	End  bool
	Data []byte
}

type Response struct {
	ID    IDType              `json:"id" msgpack:"id"`
	Type  FileSystemOperation `json:"type" msgpack:"type"`
	OK    bool                `json:"ok" msgpack:"ok"`
	Error string              `json:"error" msgpack:"error"`
	Data  []byte
}

type ParsedResponse struct {
	ID    IDType
	Type  FileSystemOperation
	OK    bool
	Error error
	Data  interface{}
}

type Request struct {
	ID   IDType              `json:"id" msgpack:"id"`
	Type FileSystemOperation `json:"type" msgpack:"type"`
	Data []byte
}

type OpenRequest struct {
	Path  string      `json:"path" msgpack:"path"`
	Flags int         `json:"flags" msgpack:"flags"`
	Perm  os.FileMode `json:"perm" msgpack:"perm"`
}

type FileOperation struct {
	FD IDType `json:"fd" msgpack:"fd"`
}

type OpenResponse struct {
	FD IDType `json:"fd" msgpack:"fd"`
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
	Path string `json:"path" msgpack:"path"`
}
