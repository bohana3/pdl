package main

import (
	"github.com/bohana3/pdl"
)

const testURL = "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2018-03.parquet"

func main() {
	pdl.Download(testURL, "green_tripdata_2018-03.parquet")
}
