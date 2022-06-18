package pdl

import (
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"

	"strings"

	"github.com/bohana3/pdl/chunker"
)

const chunkSize = 32768 //32KB

//main function that download file from URL
func Download(url, fileName string) error {
	var wg sync.WaitGroup
	urlInfo, err := getUrlInfo(url)
	if err != nil {
		return err
	}
	log.Printf("%s: size=%d, etag=%s", url, urlInfo.ContentLength, urlInfo.ETag)
	err = createEmptyFile(fileName, urlInfo.ContentLength)
	if err != nil {
		return err
	}
	chunks, err := chunker.Split(urlInfo.ContentLength, chunkSize)
	if err != nil {
		return err
	}
	wg.Add(len(chunks))

	for _, chunk := range chunks {
		go func(chunk chunker.Chunk) {
			err := downloadPart(url, chunk.Start, chunk.End, fileName)
			if err != nil {
				log.Fatalf("downloadPart failed: %s", err.Error())
			}
			wg.Done()
		}(chunk)
	}

	wg.Wait()

	//checksum
	md5, err := getMd5(fileName)
	if err != nil {
		log.Printf("getMd5 failed: path=%s", fileName)
	}
	if urlInfo.ETag != md5 {
		log.Printf("checksum failed: etag=%s, md5=%s", urlInfo.ETag, md5)
	}

	return nil
}

type UrlInfo struct {
	ContentLength int64
	ETag          string
}

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

// compute file downloaded hash
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
