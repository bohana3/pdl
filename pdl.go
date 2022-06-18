package pdl

import (
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/bohana3/pdl/chunker"
)

// Download that downloads a file from a URL using chunks
func Download(url, fileName string, maxGoroutines *int, chunkSize *int64, retries *int, timeout int) error {
	log.Printf("Download starts: url=%s, fileName=%s, maxGoroutines=%d, chunkSize=%d, retries=%d, timeout=%d",
		url, fileName, *maxGoroutines, *chunkSize, *retries, timeout)

	urlInfo, err := getUrlInfo(url)
	if err != nil {
		return err
	}
	log.Printf("%s: size=%d, etag=%s", url, urlInfo.ContentLength, urlInfo.ETag)
	err = createEmptyFile(fileName, urlInfo.ContentLength)
	if err != nil {
		return err
	}
	chunks, err := chunker.Split(urlInfo.ContentLength, *chunkSize)
	if err != nil {
		return err
	}

	errGp := new(errgroup.Group)
	chunkCh := make(chan chunker.Chunk)

	// Adding routines and run them
	for i := 0; i < *maxGoroutines; i++ {
		errGp.Go(func() error { return downloadAll(chunkCh, url, *retries, timeout, fileName) })
	}

	// Spreading chunks to free goroutines
	for _, chunk := range chunks {
		chunkCh <- chunk
	}

	// Wait for all goroutines to finish
	close(chunkCh)

	if err := errGp.Wait(); err != nil {
		log.Printf("one of the goroutines failed: %s", err.Error())
		err := os.Remove(fileName)
		if err != nil {
			log.Printf("unable to delete file '%s': %s", fileName, err.Error())
		}
	}

	// Checksum
	md5, err := getMd5(fileName)
	if err != nil {
		log.Printf("getMd5 failed: path=%s", fileName)
	}
	if urlInfo.ETag != md5 {
		log.Printf("checksum failed: etag=%s, md5=%s", urlInfo.ETag, md5)
	}

	log.Printf("Download url '%s' succeeded to file '%s'", url, fileName)
	return nil
}

// downloadAll that downloads all files chunks and writes them into file
func downloadAll(chunksChan chan chunker.Chunk, url string, retries int, timeout int, fileName string) error {
	var err error
	for chunk := range chunksChan {
		for retries > 0 {
			err = downloadPart(url, retries, chunk.Start, chunk.End, fileName, timeout)
			if err != nil {
				log.Printf("downloadPart failed: %s", err.Error())
				retries -= 1
			} else {
				break
			}
		}
	}
	return err
}

type UrlInfo struct {
	ContentLength int64
	ETag          string
}

// getUrlInfo retrieves the file length and hash
func getUrlInfo(url string) (*UrlInfo, error) {
	resp, err := http.Head(url)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: bad status - %s", url, resp.Status)
	}

	urlInfo := &UrlInfo{
		ContentLength: resp.ContentLength,
		ETag:          strings.Replace(resp.Header["Etag"][0], "\"", "", -1),
	}
	return urlInfo, nil
}

// downloadPart downloads a file chunk and writes it into file
func downloadPart(url string, retries int, start, end int64, path string, timeout int) error {

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

	var httpClient = &http.Client{
		Timeout: time.Duration(timeout) * time.Second,
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != (http.StatusOK | http.StatusPartialContent) {
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

// getMd5 computes file hash
func getMd5(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", h.Sum(nil)), nil
}
