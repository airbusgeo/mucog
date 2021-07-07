package main

import (
	"fmt"

	"github.com/airbusgeo/godal"
)

func main() {
	godal.RegisterInternalDrivers()
	generate1("s1.tif", [4]byte{1, 2, 3, 4})
	generate1("s2.tif", [4]byte{5, 6, 7, 8})
	generate2("p1.tif", "PIXEL", [8]byte{1, 2, 3, 4, 5, 6, 7, 8})
	generate2("p2.tif", "PIXEL", [8]byte{9, 10, 11, 12, 13, 14, 15, 16})
	generate2("b1.tif", "BAND", [8]byte{1, 2, 3, 4, 5, 6, 7, 8})
	generate2("b2.tif", "BAND", [8]byte{9, 10, 11, 12, 13, 14, 15, 16})
}

func fillbuf(buf []byte, val byte) {
	for i := range buf {
		buf[i] = val
	}
}
func generate1(fname string, vals [4]byte) {
	ds, _ := godal.Create(godal.GTiff, fname, 1, godal.Byte, 128, 128, godal.CreationOption(
		"TILED=YES", "BLOCKXSIZE=64", "BLOCKYSIZE=64",
	))
	ds.SetGeoTransform([6]float64{0, 0.001, 0, 0, 0, -0.001})
	sr, _ := godal.NewSpatialRefFromEPSG(4326)
	ds.SetSpatialRef(sr)
	sr.Close()
	buf := make([]byte, 64*64)
	fillbuf(buf, vals[0])
	ds.Write(0, 0, buf, 64, 64)
	fillbuf(buf, vals[1])
	ds.Write(64, 0, buf, 64, 64)
	fillbuf(buf, vals[2])
	ds.Write(0, 64, buf, 64, 64)
	fillbuf(buf, vals[3])
	ds.Write(64, 64, buf, 64, 64)
	ds.BuildOverviews(godal.Levels(2))
	ds.Close()
}
func generate2(fname string, interleave string, vals [8]byte) {
	ds, _ := godal.Create(godal.GTiff, fname, 2, godal.Byte, 128, 128, godal.CreationOption(
		"TILED=YES", "BLOCKXSIZE=64", "BLOCKYSIZE=64", fmt.Sprintf("INTERLEAVE=%s", interleave),
	))
	ds.SetGeoTransform([6]float64{0, 0.001, 0, 0, 0, -0.001})
	sr, _ := godal.NewSpatialRefFromEPSG(4326)
	ds.SetSpatialRef(sr)
	sr.Close()
	buf := make([]byte, 64*64)
	bnds := ds.Bands()
	fillbuf(buf, vals[0])
	bnds[0].Write(0, 0, buf, 64, 64)
	fillbuf(buf, vals[1])
	bnds[0].Write(64, 0, buf, 64, 64)
	fillbuf(buf, vals[2])
	bnds[0].Write(0, 64, buf, 64, 64)
	fillbuf(buf, vals[3])
	bnds[0].Write(64, 64, buf, 64, 64)

	fillbuf(buf, vals[4])
	bnds[1].Write(0, 0, buf, 64, 64)
	fillbuf(buf, vals[5])
	bnds[1].Write(64, 0, buf, 64, 64)
	fillbuf(buf, vals[6])
	bnds[1].Write(0, 64, buf, 64, 64)
	fillbuf(buf, vals[7])
	bnds[1].Write(64, 64, buf, 64, 64)
	ds.BuildOverviews(godal.Levels(2))
	ds.Close()
}
