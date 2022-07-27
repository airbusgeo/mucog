package mucog_test

import (
	"bytes"
	"fmt"

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
	subdir1 = []uint8{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3}
	subdir2 = []uint8{5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}
)

func generateData(fname string, vals [4]byte) error {
	ds, err := godal.Create(godal.GTiff, fname, 1, godal.Byte, 128, 128, godal.CreationOption(
		"TILED=YES", "BLOCKXSIZE=64", "BLOCKYSIZE=64",
	))
	if err != nil {
		return err
	}
	if err = ds.SetGeoTransform([6]float64{0, 0.001, 0, 0, 0, -0.001}); err != nil {
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
	buf := make([]byte, 64*64)
	fillbuf(buf, vals[0])
	if err = ds.Write(0, 0, buf, 64, 64); err != nil {
		return err
	}
	fillbuf(buf, vals[1])
	if err = ds.Write(64, 0, buf, 64, 64); err != nil {
		return err
	}
	fillbuf(buf, vals[2])
	if err = ds.Write(0, 64, buf, 64, 64); err != nil {
		return err
	}
	fillbuf(buf, vals[3])
	if err = ds.Write(64, 64, buf, 64, 64); err != nil {
		return err
	}
	if err = ds.BuildOverviews(godal.Levels(2, 4, 8)); err != nil {
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

	if err := generateData(filePath1, [4]byte{1, 2, 3, 4}); err != nil {
		t.Errorf("TestMucog.generateData: %v", err)
	}
	if err := generateData(filePath2, [4]byte{5, 6, 7, 8}); err != nil {
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
