package iterator_test

import (
	. "github.com/airbusgeo/mucog/iterator"
	"strings"
	"testing"
)

func testEncodeDecode(x, y int32) bool {
	nx, ny := DecodePair(EncodePair(x, y))
	return nx == x && ny == y
}

func TestEncodeDecodePair(t *testing.T) {
	if !testEncodeDecode(0, 0) {
		t.Error("EncodeDecodePair(0, 0)")
	}
	if !testEncodeDecode(0, 10) {
		t.Error("EncodeDecodePair(0, 10)")
	}
	if !testEncodeDecode(10, 0) {
		t.Error("EncodeDecodePair(10, 0)")
	}
	if !testEncodeDecode(10, 5) {
		t.Error("EncodeDecodePair(10, 5)")
	}
}

func TestRangeIterator(t *testing.T) {
	for min := 0; min < 2; min++ {
		for r := 0; r < 2; r++ {
			for idx := 0; idx < 3; idx++ {
				it := NewRangeIterator(idx, min, min+r)
				i := 0
				indices := []*int{nil, nil, nil, nil}
				for it.Init(indices); it.Next(); i++ {
				}
				if i != r {
					t.Errorf("wrong number of iterations %d expected %d", i, r)
				}
				if indices[idx] == nil {
					t.Error("wrong idx")
				} else if *indices[idx] != min+r-1 {
					t.Errorf("wrong idx: %d expected %d", *indices[idx], min+r-1)
				}
			}
		}
	}
}

func TestValuesIterator(t *testing.T) {
	for idx := 0; idx < 3; idx++ {
		for min := 0; min < 2; min++ {
			for r := 0; r < 4; r++ {
				var values []int
				for i := min; i < min+r; i += 2 {
					values = append(values, i)
				}
				it := NewValuesIterator(idx, values)
				i := 0
				indices := []*int{nil, nil, nil, nil}
				for it.Init(indices); it.Next(); i++ {
				}
				if i != len(values) {
					t.Errorf("wrong number of iterations %d expected %d", i, len(values))
				}
				if indices[idx] == nil {
					t.Error("wrong idx")
				} else if len(values) > 0 && *indices[idx] != values[len(values)-1] {
					t.Errorf("wrong idx: %d expected %d", *indices[idx], min+r-1)
				}
			}
		}
	}
}

func TestTilesIterator(t *testing.T) {
	for minX := int32(0); minX < 2; minX++ {
		for sX := int32(0); sX < 3; sX++ {
			for minY := int32(0); minY < 2; minY++ {
				for sY := int32(0); sY < 3; sY++ {
					minMax := [4]int32{minX, minX + sX, minY, minY + sY}
					it := NewTileIterator(IDX_TILE, [][4]int32{minMax})
					i := int32(0)
					indices := []*int{nil, nil, nil, nil}
					z := 0
					indices[IDX_ZOOM] = &z
					for it.Init(indices); it.Next(); i++ {
						x, y := DecodePair(*indices[IDX_TILE])
						if x < minMax[0] || x >= minMax[1] || y < minMax[2] || y >= minMax[3] {
							t.Errorf("wrong iteration, got: %d %d, expected in %v", x, y, minMax)
						}
					}
					if i != sX*sY {
						t.Errorf("wrong number of iterations %d expected %d with %v", i, sX*sY, minMax)
					}
				}
			}
		}
	}
}

func TestIterators(t *testing.T) {
	if _, err := NewIteratorsFromString("R>B>T", 0, 0, nil); err == nil || !strings.Contains(err.Error(), "must have four level of iterations") {
		t.Errorf("TestLevelOfIterator: %v", err)
	}
	if _, err := NewIteratorsFromString("R>B>T>K", 0, 0, nil); err == nil || !strings.Contains(err.Error(), "unknown key") {
		t.Errorf("TestUnknownKey: %v", err)
	}
	if _, err := NewIteratorsFromString("R>B>R>Z", 0, 0, nil); err == nil || !strings.Contains(err.Error(), "defined twice") {
		t.Errorf("TestDefinedTwice: %v", err)
	}
	if _, err := NewIteratorsFromString("R>B>T>Z", 0, 0, nil); err == nil || !strings.Contains(err.Error(), "cannot be defined before") {
		t.Errorf("TestZoomDefinedBeforeTile: %v", err)
	}
	if _, err := NewIteratorsFromString("R>B>Z>T", 0, 0, nil); err != nil {
		t.Error(err)
	}
	if its, err := NewIteratorsFromString("R>B=2>Z>T", 0, 0, nil); err != nil {
		t.Error(err)
	} else if it, ok := its[1].(*ValuesIterator); !ok {
		t.Error("Not ValuesIterator")
	} else if len(it.Values) != 0 {
		t.Errorf("wrong values: %v", it.Values)
	}
	if its, err := NewIteratorsFromString("R>B=2>Z>T", 10, 10, nil); err != nil {
		t.Error(err)
	} else if it, ok := its[1].(*ValuesIterator); !ok {
		t.Error("Not ValuesIterator")
	} else if len(it.Values) != 1 || it.Values[0] != 2 {
		t.Errorf("wrong values: %v", it.Values)
	}
	if its, err := NewIteratorsFromString("R>B=2:>Z>T", 10, 10, nil); err != nil {
		t.Error(err)
	} else if it, ok := its[1].(*RangeIterator); !ok {
		t.Error("Not RangeIterator")
	} else if it.Start != 2 || it.End != 10 {
		t.Errorf("wrong values: %d, %d", it.Start, it.End)
	}
	if its, err := NewIteratorsFromString("R>B=:2>Z>T", 10, 10, nil); err != nil {
		t.Error(err)
	} else if it, ok := its[1].(*RangeIterator); !ok {
		t.Error("Not RangeIterator")
	} else if it.Start != 0 || it.End != 2 {
		t.Errorf("wrong values: %d, %d", it.Start, it.End)
	}
	if its, err := NewIteratorsFromString("R>B=3:11>Z>T", 10, 10, nil); err != nil {
		t.Error(err)
	} else if it, ok := its[1].(*RangeIterator); !ok {
		t.Error("Not RangeIterator")
	} else if it.Start != 3 || it.End != 10 {
		t.Errorf("wrong values: %d, %d", it.Start, it.End)
	}
}
