package pdl

import (
	"os"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetUrlInfo_ContentLength(t *testing.T) {
	const testURL = "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2018-03.parquet"
	urlInfo, err := getUrlInfo(testURL)
	require.NoError(t, err)
	expected := int64(13304186)
	require.Equal(t, expected, urlInfo.ContentLength)
}

func TestGetUrlInfo_Etag(t *testing.T) {
	const testURL = "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2018-03.parquet"
	urlInfo, err := getUrlInfo(testURL)
	require.NoError(t, err)
	expected := "6ab45198235ba77c5b99a2e68a030d7f"
	require.Equal(t, expected, urlInfo.ETag)
}

func TestGetMd5(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	t.Logf("Current test filename: %s", filename)
	md5, err := getMd5("testResources/test.txt")
	require.NoError(t, err)
	require.Equal(t, "e80b5017098950fc58aad83c8c14978e", md5)
}

func TestDownloadPart(t *testing.T) {
	const testURL = "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2018-03.parquet"
	const testFile = "tests_resources/part"
	err := createEmptyFile(testFile, 50)
	require.NoError(t, err)

	err = downloadPart(testURL, 0, 50, testFile)
	require.NoError(t, err)

	//cleanup
	os.Remove(testFile)
}

func TestCreateEmptyFile(t *testing.T) {
	const testFile = "tests_resources/empty"
	testFileSize := int64(50)
	err := createEmptyFile(testFile, testFileSize)
	require.NoError(t, err)
	f, err := os.Stat(testFile)
	require.NoError(t, err)

	require.Equal(t, testFileSize, f.Size())

	//cleanup
	os.Remove(testFile)
}
