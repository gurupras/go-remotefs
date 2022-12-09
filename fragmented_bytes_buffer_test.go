package remotefs

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestFragmentedBufferRead(t *testing.T) {
	require := require.New(t)

	count := 100

	type Tmp struct {
		Id    int
		IdStr string
	}
	buf := newFragmentedBytesBuffer()

	expectedObjects := make([]Tmp, 0)
	for idx := 0; idx < count; idx++ {
		// We use msgpack to test this. msgpack uses an internal buffer of 4K when it attempts to decode
		// As a result, we're going to need some large data buffers so that it doesn't read all of the data
		// in one shot
		tmp := Tmp{idx, fmt.Sprintf("%01000d", idx)}
		expectedObjects = append(expectedObjects, tmp)

		b, err := msgpack.Marshal(&tmp)
		require.Nil(err)
		// Split this buffer into two for better testing
		_, err = buf.Write(b[:len(b)/2])
		require.Nil(err)
		_, err = buf.Write(b[len(b)/2:])
		require.Nil(err)
	}

	decoder := msgpack.NewDecoder(buf)
	for idx := 0; idx < count; idx++ {
		expected := expectedObjects[idx]
		var got Tmp
		err := decoder.Decode(&got)
		require.Nil(err)
		require.Equal(expected, got)
	}
}
