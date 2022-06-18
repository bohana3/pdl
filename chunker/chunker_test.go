package chunker

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

var splitCases = []struct { // anonymous struct
	size      int64
	chunkSize int64
	chunks    []Chunk
}{
	{7, 3, []Chunk{{0, 2}, {3, 5}, {6, 6}}},
	// ...
}

// Table testing
func TestSplit(t *testing.T) {
	for _, tc := range splitCases {
		name := fmt.Sprintf("%d-%d", tc.size, tc.chunkSize)
		t.Run(name, func(t *testing.T) {
			chunks, err := Split(tc.size, tc.chunkSize)
			require.NoError(t, err)
			require.Equal(t, tc.chunks, chunks)
		})
	}
}
