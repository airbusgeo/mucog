package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/airbusgeo/mucog"

	"github.com/google/tiff"
	_ "github.com/google/tiff/bigtiff"
)

func main() {
	ctx := context.Background()
	err := run(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	outfile := flag.String("output", "out.tif", "destination file")
	sbigtiff := flag.String("bigtiff", "auto", "force bigtiff (yes|no|auto)")
	pattern := flag.String("pattern", mucog.MUCOGPattern, "pattern to use for data interlacing (default: \""+mucog.MUCOGPattern+"\")")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [options] dataset.tif [dataset_2.tif...]\nOptions:\n", filepath.Base(os.Args[0]))
		flag.PrintDefaults()
		return fmt.Errorf("")
	}

	totalSize := int64(0)
	multicog := mucog.New()

	for _, input := range args {
		topFile, err := os.Open(input)
		if err != nil {
			return fmt.Errorf("open %s: %w", args[0], err)
		}
		defer topFile.Close()
		st, err := topFile.Stat()
		if err != nil {
			return fmt.Errorf("stat %s: %w", args[0], err)
		}
		totalSize += st.Size()

		tif, err := tiff.Parse(topFile, nil, nil)
		if err != nil {
			log.Fatal(fmt.Errorf("parse %s: %w", input, err))
		}
		tifmifds, err := mucog.LoadTIFF(tif)
		if err != nil {
			log.Fatal(fmt.Errorf("load %s: %w", input, err))
		}
		if len(tifmifds) == 1 && tifmifds[0].DocumentName == "" {
			tifmifds[0].DocumentName = path.Base(input)
			tifmifds[0].DocumentName = strings.TrimSuffix(
				tifmifds[0].DocumentName, filepath.Ext(tifmifds[0].DocumentName))
		}
		for _, mifd := range tifmifds {
			multicog.AppendIFD(mifd)
		}
	}

	bigtiff := totalSize > int64(^uint32(0))
	switch *sbigtiff {
	case "yes":
		bigtiff = true
	case "no":
		bigtiff = false
	case "auto":
	default:
		return fmt.Errorf("invalid bigtiff option")
	}

	out, err := os.Create(*outfile)
	if err != nil {
		return fmt.Errorf("create %s: %w", *outfile, err)
	}

	err = multicog.Write(out, bigtiff, *pattern)
	if err != nil {
		log.Fatal(err)
	}
	err = out.Close()
	if err != nil {
		return fmt.Errorf("close %s: %w", *outfile, err)
	}
	return nil
}
