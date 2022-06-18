package main

import (
	"flag"

	"github.com/bohana3/pdl"
)

const testURL = "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2018-03.parquet"

func main() {
	maxGoRoutines := flag.Int("maxgoroutines", 100, "max go routines")
	chunkSize := flag.Int64("chunksize", 32768, "chunck size in bytes")
	flag.Parse()

	pdl.Download(testURL, "green_tripdata_2018-03.parquet", maxGoRoutines, chunkSize)
}
