package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"sync"

	"github.com/airbusgeo/godal"
	"github.com/airbusgeo/mucog"
	"github.com/google/tiff"
)

type creationOptions []string

func (i *creationOptions) String() string {
	return "gdal creation option passed as KEY=VALUE"
}

func (i *creationOptions) Set(value string) error {
	*i = append(*i, value)
	return nil
}

type jp2 struct {
	file string
	opts creationOptions
}

func main() {
	var createOpts creationOptions
	flag.Var(&createOpts, "co", "gtiff creation option (may be repeated)")
	workers := flag.Int("workers", 4, "number of concurrent workers")
	flag.Parse()

	jp2s := make(chan jp2)

	wg := sync.WaitGroup{}
	wg.Add(*workers)
	for i := 0; i < *workers; i++ {
		go func() {
			defer wg.Done()
			err := worker(jp2s)
			if err != nil {
				log.Fatal(err)
			}
		}()
	}

	for _, jp2file := range flag.Args() {
		jp2s <- jp2{jp2file, createOpts}
	}
	close(jp2s)

	wg.Wait()
}

func worker(in <-chan jp2) error {
	for jp2file := range in {
		err := process(jp2file.file, jp2file.opts)
		if err != nil {
			return err
		}
	}
	return nil
}

func process(file string, copts creationOptions) error {

	defer log.Printf("created %s", file+".tif")
	tif, err := ioutil.TempFile(".", "")
	if err != nil {
		return err
	}
	tif.Close()
	//kakamutex.Lock()
	jp2, err := godal.Open(file)
	if err != nil {
		//kakamutex.Unlock()
		return fmt.Errorf("open %s: %w", file, err)
	}
	opts := []string{
		"-of", "GTIFF",
		"-srcnodata", "0",
		"-ot", "UInt16",
		"-te", "303700", "4793920", "406100", "4896320",
	}
	for _, opt := range copts {
		opts = append(opts, "-co", opt)
	}
	tifds, err := godal.Warp(tif.Name(), []*godal.Dataset{jp2}, opts)
	if err != nil {
		//kakamutex.Unlock()
		return fmt.Errorf("warp %s->%s: %w", file, tif.Name(), err)
	}
	jp2.Close()
	//kakamutex.Unlock()
	tifname := tif.Name()
	tsx, tsy := tifds.Bands()[0].Structure().BlockSizeX, tifds.Bands()[0].Structure().BlockSizeY
	ovrlevel := 2
	multicog := mucog.New()
	var topifd *mucog.IFD

	/*
		{
			tifds.Close()
			f, err := os.Open(tif.Name())
			tif, _ := tiff.Parse(f, nil, nil)
			tifmifds, err := mucog.LoadTIFF(tif)
			if err != nil {
				log.Fatal(err)
			}
			for _, mifd := range tifmifds {
				multicog.AppendIFD(mifd)
			}

			out, _ := os.Create("out.tif")
			err = multicog.Write(out)
			if err != nil {
				log.Fatal(err)
			}
			out.Close()
			return nil
		}
	*/

	var totalSize int64
	for {
		defer os.Remove(tifname)
		if tifds.Structure().SizeX <= tsx && tifds.Structure().SizeY <= tsy {
			tifds.Close()
			tiffile, err := os.Open(tifname)
			if err != nil {
				return fmt.Errorf("open %s: %w", tifname, err)
			}
			st, err := tiffile.Stat()
			if err != nil {
				return fmt.Errorf("stat %s: %w", tiffile.Name(), err)
			}
			totalSize += st.Size()

			tiffstruct, err := tiff.Parse(tiffile, nil, nil)
			if err != nil {
				return fmt.Errorf("tiff parse %s: %w", tifname, err)
			}
			tifmifds, err := mucog.LoadTIFF(tiffstruct)
			if err != nil {
				return fmt.Errorf("mucog load %s: %w", tifname, err)
			}
			ifd := tifmifds[0]
			if ovrlevel != 2 {
				ifd.SubfileType = mucog.SubfileTypeReducedImage
				ifd.GeoAsciiParamsTag = ""
				ifd.GeoDoubleParamsTag = nil
				ifd.GeoKeyDirectoryTag = nil
				ifd.ModelPixelScaleTag = nil
				ifd.ModelTiePointTag = nil
				ifd.ModelTransformationTag = nil
				topifd.SubIFDs = append(topifd.SubIFDs, ifd)
			}
			break
		}
		gt, err := tifds.GeoTransform()
		if err != nil {
			return fmt.Errorf("geotransform failed: %w", err)
		}
		resx, resy := math.Abs(gt[1])*2.0, math.Abs(gt[5])*2.0
		ovrname := tifname + ".ovr"
		opts := []string{
			"-of", "GTIFF",
			"-tr", fmt.Sprintf("%.12g", resx), fmt.Sprintf("%.12g", resy),
		}
		for _, opt := range copts {
			opts = append(opts, "-co", opt)
		}
		ovrds, err := godal.Warp(ovrname, []*godal.Dataset{tifds}, opts)
		if err != nil {
			return fmt.Errorf("ovr %d %s: %w", ovrlevel, ovrname, err)
		}

		err = tifds.Close()
		if err != nil {
			return fmt.Errorf("ds.close %s: %w", tifname, err)
		}

		tiffile, err := os.Open(tifname)
		if err != nil {
			return fmt.Errorf("open %s: %w", tifname, err)
		}
		tiffstruct, err := tiff.Parse(tiffile, nil, nil)
		if err != nil {
			return fmt.Errorf("tiff parse %s: %w", tifname, err)
		}
		tifmifds, err := mucog.LoadTIFF(tiffstruct)
		if err != nil {
			return fmt.Errorf("mucog load %s: %w", tifname, err)
		}
		ifd := tifmifds[0]
		if ovrlevel != 2 {
			ifd.SubfileType = mucog.SubfileTypeReducedImage
			ifd.GeoAsciiParamsTag = ""
			ifd.GeoDoubleParamsTag = nil
			ifd.GeoKeyDirectoryTag = nil
			ifd.ModelPixelScaleTag = nil
			ifd.ModelTiePointTag = nil
			ifd.ModelTransformationTag = nil
			topifd.SubIFDs = append(topifd.SubIFDs, ifd)
		} else {
			topifd = ifd
			multicog.AppendIFD(topifd)
		}

		tifname = ovrname
		tifds = ovrds
		ovrlevel *= 2
	}
	outfile, err := os.Create(file + ".tif")
	if err != nil {
		return fmt.Errorf("create %s: %w", file+".tif", err)
	}

	bigtiff := totalSize > int64(^uint32(0))

	err = multicog.Write(outfile, bigtiff, mucog.MUCOGPattern)
	if err != nil {
		return fmt.Errorf("multicog.write %s: %w", outfile.Name(), err)
	}
	err = outfile.Close()
	if err != nil {
		return fmt.Errorf("close %s: %w", outfile.Name(), err)
	}

	return nil
}
