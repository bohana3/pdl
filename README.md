# pdl
Parallel Downloader in Go (workshop by Miki on 16/06/22)

# features

* Use the etag field which is the MD5 signature to validate your download
* Test everything
* Add a command line parameter to limit the number of downloading goroutintes
* Add a command line parameter to set the chunk size
* Support retrying of a failed download
* Add command line parameter to control number of retries
* Add connection timeout
* Cancel all goroutines on error & delete the file (See errgroup)
