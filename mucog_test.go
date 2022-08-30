package mucog_test

import (
	"bytes"
	"fmt"
	"strconv"

	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/airbusgeo/godal"
	"github.com/airbusgeo/mucog"
	"github.com/google/tiff"
)

var (
	subdir1 = []uint8{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}
	subdir2 = []uint8{5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6}
)

func generateData(fname string, vals [][]byte, offset [2]float64) error {
	bs := 64
	ds, err := godal.Create(godal.GTiff, fname, 1, godal.Byte, bs*len(vals), bs*len(vals[0]), godal.CreationOption(
		"TILED=YES", fmt.Sprintf("BLOCKXSIZE=%d", bs), fmt.Sprintf("BLOCKYSIZE=%d", bs),
	))
	if err != nil {
		return err
	}
	if err = ds.SetGeoTransform([6]float64{offset[0] * float64(bs), 1, 0, offset[1] * float64(bs), 0, -1}); err != nil {
		return err
	}
	sr, err := godal.NewSpatialRefFromEPSG(4326)
	if err != nil {
		return err
	}
	if err = ds.SetSpatialRef(sr); err != nil {
		return err
	}
	sr.Close()
	// Fill Data
	buf := make([]byte, bs*bs)
	for i, vs := range vals {
		for j, v := range vs {
			fillbuf(buf, v)
			if err = ds.Write(bs*i, bs*j, buf, bs, bs); err != nil {
				return err
			}
		}
	}
	if err = ds.BuildOverviews(godal.Levels(2, 4, 8, 24)); err != nil { // 24 is for non-integer zoom factor
		return err
	}
	if err = ds.Close(); err != nil {
		return err
	}
	return nil
}

func fillbuf(buf []byte, val byte) {
	for i := range buf {
		buf[i] = val
	}
}

func loadMucog(files []*os.File, multicog *mucog.MultiCOG) error {
	for _, file := range files {
		tif, err := tiff.Parse(file, nil, nil)
		if err != nil {
			log.Fatal(fmt.Errorf("parse %s: %w", file.Name(), err))
		}
		tifmifds, err := mucog.LoadTIFF(tif)
		if err != nil {
			log.Fatal(fmt.Errorf("load %s: %w", file.Name(), err))
		}
		if len(tifmifds) == 1 && tifmifds[0].DocumentName == "" {
			tifmifds[0].DocumentName = path.Base(file.Name())
			tifmifds[0].DocumentName = strings.TrimSuffix(
				tifmifds[0].DocumentName, filepath.Ext(tifmifds[0].DocumentName))
		}
		for _, mifd := range tifmifds {
			multicog.AppendIFD(mifd)
		}
		//topFile.Close()
	}
	return nil
}

// Get Offset
func offsets(d *godal.Dataset) ([]int, error) {
	// TopLevel
	band := d.Bands()[0]
	ifdOffset, err := strconv.Atoi(band.Metadata("IFD_OFFSET", godal.Domain("TIFF")))
	if err != nil {
		return nil, err
	}
	ifdOffsets := []int{ifdOffset}

	// Overviews
	for i, ovr := range band.Overviews() {
		if i == 0 {
			if ovr.Structure().SizeX > band.Structure().SizeX || ovr.Structure().SizeY > band.Structure().SizeY {
				return nil, fmt.Errorf("first overview has larger dimension than main band")
			}
		} else {
			prevOvr := band.Overviews()[i-1]
			if ovr.Structure().SizeX > prevOvr.Structure().SizeX || ovr.Structure().SizeY > prevOvr.Structure().SizeY {
				return nil, fmt.Errorf("overview of index %d has larger dimension than overview of index %d", i, i-1)
			}
		}
		if ifdOffset, err = strconv.Atoi(ovr.Metadata("IFD_OFFSET", godal.Domain("TIFF"))); err != nil {
			return nil, err
		}
		ifdOffsets = append(ifdOffsets, ifdOffset)
	}
	return ifdOffsets, nil
}

func checkMucog(f string) error {
	d1, err := godal.Open("GTIFF_DIR:1:" + f)
	if err != nil {
		return err
	}
	defer d1.Close()

	d2, err := godal.Open("GTIFF_DIR:2:" + f)
	if err != nil {
		return err
	}
	defer d2.Close()

	data := make([]uint8, 128)
	if err := d1.Read(0, 0, data, 1, 128); err != nil {
		return err
	}

	if !bytes.Equal(data, subdir1) {
		return fmt.Errorf("subdir1 content mismatch")
	}

	data = make([]uint8, 128)
	if err := d2.Read(0, 0, data, 1, 128); err != nil {
		return err
	}

	if !bytes.Equal(data, subdir2) {
		return fmt.Errorf("subdir2 content mismatch")
	}

	// Overviews
	ovrCount := len(d1.Bands()[0].Overviews())
	if len(d1.Bands()[0].Overviews()) != ovrCount {
		return fmt.Errorf("d1 and d2 do not have the same number of overviews")
	}

	// Offsets
	offset1, err := offsets(d1)
	if err != nil {
		return err
	}
	offset2, err := offsets(d2)
	if err != nil {
		return err
	}

	if offset1[0] > offset2[0] {
		return fmt.Errorf("d1 should be before d2")
	}

	for i := 0; i < ovrCount-1; i++ {
		if offset1[i] > offset1[i+1] || offset2[i] > offset2[i+1] {
			return fmt.Errorf("overview %d should be before overview %d", i, i+1)
		}
		if offset1[i] > offset2[i+1] {
			return fmt.Errorf("overview %d dataset 1 should be before overview %d dataset 2", i, i+1)
		}
	}

	offsets1, err := offsets(d1)
	if err != nil {
		return err
	}
	offsets2, err := offsets(d2)
	if err != nil {
		return err
	}

	if offsets1[0] > offsets2[0] {
		return fmt.Errorf("d1 should be before d2")
	}

	for i := 0; i < ovrCount-1; i++ {
		if offsets1[i] > offsets1[i+1] || offsets2[i] > offsets2[i+1] {
			return fmt.Errorf("overview %d should be before overview %d", i, i+1)
		}
		if offsets1[i] > offsets2[i+1] {
			return fmt.Errorf("overview %d dataset 1 should be before overview %d dataset 2", i, i+1)
		}
	}
	return nil
}

func TestMucog(t *testing.T) {
	godal.RegisterAll()

	var file1, file2 *os.File
	var err error

	filePath1 := path.Join(os.TempDir(), "s1.tif")
	filePath2 := path.Join(os.TempDir(), "s2.tif")
	resultFilePath := path.Join(os.TempDir(), "mucog.tif")

	defer func() {
		os.RemoveAll(filePath1)
		os.RemoveAll(filePath2)
		os.RemoveAll(resultFilePath)
		file1.Close()
		file2.Close()
	}()

	if err := generateData(filePath1, [][]byte{{1, 2}, {4, 5}}, [2]float64{1, 1}); err != nil {
		t.Errorf("TestMucog.generateData: %v", err)
	}
	if err := generateData(filePath2, [][]byte{{5, 6, 7, 8}, {7, 8, 9, 10}}, [2]float64{0, 0}); err != nil {
		t.Errorf("TestMucog.generateData: %v", err)
	}

	file1, err = os.Open(filePath1)
	if err != nil {
		t.Errorf("TestMucog.Openfile1: %v", err)
	}
	file2, err = os.Open(filePath2)
	if err != nil {
		t.Errorf("TestMucog.Openfile2: %v", err)
	}

	multiCOG := mucog.New()
	if err := loadMucog([]*os.File{file1, file2}, multiCOG); err != nil {
		t.Errorf("TestMucog.loadMucog: %v", err)
	}

	out, err := os.Create(resultFilePath)
	if err != nil {
		t.Errorf("TestMucog.Create: %v", err)
	}
	err = multiCOG.Write(out, false, mucog.MUCOGPattern)
	if err != nil {
		t.Errorf("TestMucog.Write: %v", err)
	}
	err = out.Close()
	if err != nil {
		t.Errorf("TestMucog.Close: %v", err)
	}

	if err := checkMucog(resultFilePath); err != nil {
		t.Errorf("TestMucog.checkMucog: %v", err)
	}
}
