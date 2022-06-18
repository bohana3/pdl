package main

import (
	"flag"
	"log"

	"github.com/bohana3/pdl"
)

const testURL = "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2018-03.parquet"
const testFile = "green_tripdata_2018-03.parquet"
const timeout = 30 // in seconds

func main() {
	maxGoRoutines := flag.Int("maxgoroutines", 100, "max go routines")
	chunkSize := flag.Int64("chunksize", 32768, "chunck size in bytes")
	retries := flag.Int("retries", 3, "retries attempts when a request fails")

	flag.Parse()

	err := pdl.Download(testURL, testFile, maxGoRoutines, chunkSize, retries, timeout)
	if err != nil {
		log.Printf("Download failed: %s", err.Error())
	}
}
