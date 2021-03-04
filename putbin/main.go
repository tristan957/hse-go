package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/hse-project/hse-go"
)

var (
	threads int
	count   int
)

func init() {
	flag.IntVar(&threads, "t", 1, "Number of threads to run on")
	flag.IntVar(&count, "c", 1000000, "Number of keys to insert")
}

func main() {
	var wg sync.WaitGroup

	flag.Parse()

	wg.Add(threads)

	fmt.Printf("putting %d keys across %d threads\n", count, threads)

	hse.KvdbInit()
	defer hse.KvdbFini()

	p, _ := hse.NewParams()
	p.Set("kvdb.throttle_disable", "1")

	kvdb, err := hse.KvdbOpen(os.Args[5], p)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open kvdb: %s\n", err)
		os.Exit(1)
	}
	defer kvdb.Close()
	kvs, err := kvdb.KvsOpen(os.Args[6], nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open kvs: %s\n", err)
		os.Exit(1)
	}
	defer kvs.Close()

	stride := int64(count / threads)
	for i := 0; i < threads; i++ {
		start := int64(i) * stride
		s := stride
		if i == threads-1 {
			s = stride + int64(count%threads)
		}
		end := start + s
		go func(kvs *hse.Kvs, begin, finish int64) {
			defer wg.Done()
			for j := begin; j < finish; j++ {
				key := []byte(strconv.FormatInt(j, 2))
				value := []byte(strconv.FormatInt(j, 2))
				kvs.Put(key, value, nil)
			}
		}(kvs, start, end)
	}

	wg.Wait()

	kvs.Close()
	kvdb.Close()
	hse.KvdbFini()
}
