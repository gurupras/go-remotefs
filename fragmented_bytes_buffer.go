package remotefs

import (
	"io"
	"math"
)

type FragmentedBytesBuffer struct {
	bytesArray [][]byte
	arrayPos   int
	bufPos     int
}

func newFragmentedBytesBuffer() *FragmentedBytesBuffer {
	return &FragmentedBytesBuffer{
		bytesArray: make([][]byte, 0),
		arrayPos:   0,
		bufPos:     0,
	}
}

func (f *FragmentedBytesBuffer) Read(b []byte) (int, error) {
	read := 0
	var err error
	for {
		if f.arrayPos == len(f.bytesArray) {
			// We've hit the end already
			return read, io.EOF
		}
		buf := f.bytesArray[f.arrayPos]
		remaining := len(b) - read
		numBytesToReadFromBuf := int(math.Min(float64(remaining), float64(len(buf)-f.bufPos)))
		copy(b[read:], buf[f.bufPos:f.bufPos+numBytesToReadFromBuf])
		read += numBytesToReadFromBuf
		f.bufPos += numBytesToReadFromBuf
		if f.bufPos == len(buf) {
			f.arrayPos += 1
			f.bufPos = 0
		}
		if read == len(b) {
			break
		}
	}
	return read, err
}

func (f *FragmentedBytesBuffer) Write(b []byte) (int, error) {
	f.bytesArray = append(f.bytesArray, b)
	return len(b), nil
}
