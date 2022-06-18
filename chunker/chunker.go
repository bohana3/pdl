package chunker

import "fmt"

type Chunk struct {
	Start int64
	End   int64
}

func Split(size, chunkSize int64) ([]Chunk, error) {
	if size < 0 || chunkSize <= 0 {
		return nil, fmt.Errorf("size=%d, chunkSize=%d", size, chunkSize)
	}

	var chunks []Chunk
	for start := int64(0); start < size; start += chunkSize {
		end := start + chunkSize - 1
		if end > size-1 {
			end = size - 1
		}
		c := Chunk{start, end}

		chunks = append(chunks, c)
	}

	return chunks, nil
}
