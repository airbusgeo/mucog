package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/airbusgeo/godal"
	"github.com/airbusgeo/osio"

	"cloud.google.com/go/storage"
)

func main() {
	gsuri := flag.String("gsuri", "", "mucog gs uri (example: gs://bucket/path/mucog.tif")
	flag.Parse()
	if *gsuri == "" {
		log.Fatal("gsuri must be specified")
		return
	}

	{
		runtime.SetBlockProfileRate(1)
		f, err := os.Create("cpu.profile")
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	ctx := context.Background()
	godal.RegisterAll()

	storageClient, err := storage.NewClient(ctx)
	if err != nil {
		panic(err)
	}

	gcsHandler, err := osio.GCSHandle(ctx, osio.GCSClient(storageClient))
	if err != nil {
		panic(err)
	}

	adapter, err := osio.NewAdapter(gcsHandler)
	if err != nil {
		panic(err)
	}

	if err := godal.RegisterVSIAdapter("gs://", adapter); err != nil {
		panic(err)
	}

	now := time.Now()
	data := make([]uint16, 805)
	order := make(map[int]bool)
	for i := 0; i < 805; i++ {
		order[i] = true
	}

	workers := 20
	wg := sync.WaitGroup{}
	wg.Add(workers)
	imgs := make(chan int)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for i := range imgs {
				ds, err := godal.Open(*gsuri, godal.Drivers("GTiff"))
				if err != nil {
					log.Fatal(fmt.Errorf("open %s: %w", *gsuri, err))
				}

				err = ds.Bands()[0].Read(0, 0, data[i:i+1], 1, 1)
				if err != nil {
					fmt.Println(err.Error())
					return
				}
				ds.Close()
			}
		}()
	}
	for i := range order {
		imgs <- i
	}
	close(imgs)

	wg.Wait()
	log.Printf("pixel 500,500: +%v", data)
	log.Printf("took: %v", time.Since(now))
	{
		f, err := os.Create("mem.profile")
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close()
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
}
