package pdl

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/bohana3/pdl/chunker"
)

func downloadSize(url string) (int64, error) {
	resp, err := http.Head(url)
	if err != nil {
		return 0, err
	}

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("%s: bad status - %s", url, resp.Status)
	}

	return resp.ContentLength, nil
}

const chunkSize = 32768 //32KB

func Download(url, fileName string) error {
	var wg sync.WaitGroup
	size, err := downloadSize(url)
	log.Printf("%s: size=%d", url, size)
	err = createEmptyFile(fileName, size)
	if err != nil {
		return err
	}
	chunks, err := chunker.Split(size, chunkSize)
	if err != nil {
		return err
	}
	wg.Add(len(chunks))

	for _, chunk := range chunks {
		go func(chunk chunker.Chunk) {
			downloadPart(url, chunk.Start, chunk.End, fileName)
			wg.Done()
		}(chunk)
	}

	return nil
}

func downloadPart(url string, start, end int64, path string) error {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s", url, resp.Status)
	}
	// TODO: Check that content-length is end-start?

	return writeAt(path, start, resp.Body)
}

// writeAt writes data a location of file
func writeAt(path string, offset int64, r io.Reader) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0755)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return err
	}

	_, err = io.Copy(file, r)
	return err
}

// createEmptyFile creates an empty file in given size
func createEmptyFile(path string, size int64) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	file.Seek(size-1, io.SeekStart)
	file.Write([]byte{0})

	return nil
}
