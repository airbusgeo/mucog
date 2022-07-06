package iterator

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

const (
	IDX_RECORD int = iota // Datasets
	IDX_ZOOM              // Full + Overviews ie 0: Full, 1:N: Overviews
	IDX_TILE              // Block/Chunk
	IDX_BAND              // Layer
	KEY_RECORD = "R"
	KEY_ZOOM   = "Z"
	KEY_TILE   = "T"
	KEY_BAND   = "B"
)

var Names = []string{"Record", "Zoom", "Tile", "Band"}

// Iterator on integers with an Identifier
// Usage:
// var it Iterator
// for id, pval := it.ID(), it.Init(); it.Next(); {
//   fmt.Printf("It[%d] = %d", it.ID(), *pval)
// }
type Iterator interface {
	// ID returns the identifier of the iterator
	ID() int
	// Init resets the iterator and initializes indices[ID()] with the pointer on the current value (updated when Next() is called, invalid if Next()==False)
	Init(indices []*int)
	// Next updates the current value and returns True, or False if the iteration is finished.
	Next() bool
}

func InitIterators(pattern string, nbRecords, nbBands int, zoomMinMaxBlock [][4]int32) ([]*Iterators, error) {
	var iterators []*Iterators
	for _, itersS := range strings.Split(pattern, ";") {
		iters, err := NewIteratorsFromString(itersS, nbRecords, nbBands, zoomMinMaxBlock)
		if err != nil {
			return nil, err
		}
		iterators = append(iterators, iters)
	}
	return iterators, nil
}

// RangeIterator implements Iterator on a range of values from start to end (included)
type RangeIterator struct {
	id         int
	curValue   int
	Start, End int
}

// NewRangeIterator creates an Iterator on a range of values from start to end (included)
func NewRangeIterator(id, start, end int) Iterator {
	return &RangeIterator{
		id:    id,
		Start: start,
		End:   end,
	}
}

func (it *RangeIterator) Init(indices []*int) {
	it.curValue = it.Start - 1
	indices[it.id] = &it.curValue
}

func (it *RangeIterator) ID() int {
	return it.id
}

func (it *RangeIterator) Next() bool {
	if it.curValue == it.End-1 {
		return false
	}
	it.curValue++
	return true
}

// ValuesIterator implements Iterator on a slice of values
type ValuesIterator struct {
	id       int
	Values   []int
	curValue int
	curIdx   int
}

// NewValuesIterator creates an Iterator on a slice of values
func NewValuesIterator(id int, values []int) Iterator {
	return &ValuesIterator{
		id:     id,
		Values: values,
	}
}

func (it *ValuesIterator) Init(indices []*int) {
	it.curIdx = 0
	indices[it.id] = &it.curValue
}

func (it *ValuesIterator) ID() int {
	return it.id
}

func (it *ValuesIterator) Next() bool {
	if it.curIdx == len(it.Values) {
		return false
	}
	it.curValue = it.Values[it.curIdx]
	it.curIdx++
	return true
}

const (
	MIN_X int = iota
	MAX_X
	MIN_Y
	MAX_Y
)

// TileIterator creates an Iterator on the tiles of an overview level. SetOvlMinMax must be called before any other function.
type TileIterator struct {
	id               int
	curValue         int
	maxX, minY, maxY int32
	curX, curY       int32
	zoomMinMaxBlock  [][4]int32
}

// NewTileIterator creates an Iterator on the blocks of an overview level.
func NewTileIterator(id int, zoomMinMaxBlock [][4]int32) Iterator {
	return &TileIterator{
		id:              id,
		zoomMinMaxBlock: zoomMinMaxBlock,
	}
}

// Init returns a pointer on an encoded value of the block indices (see DecodePair to get x, y)
func (it *TileIterator) Init(indices []*int) {
	zoomIdx := *indices[IDX_ZOOM]
	it.curX, it.maxX = it.zoomMinMaxBlock[zoomIdx][MIN_X], it.zoomMinMaxBlock[zoomIdx][MAX_X]
	it.minY, it.maxY = it.zoomMinMaxBlock[zoomIdx][MIN_Y], it.zoomMinMaxBlock[zoomIdx][MAX_Y]
	it.curY = it.minY
	indices[it.id] = &it.curValue
}

func (it *TileIterator) ID() int {
	return it.id
}

func (it *TileIterator) Next() bool {
	it.curValue = EncodePair(it.curX, it.curY)
	if it.curX >= it.maxX || it.curY >= it.maxY {
		return false
	}
	if it.curY < it.maxY {
		it.curY++
	}
	if it.curY >= it.maxY {
		it.curX++
		it.curY = it.minY
	}
	return true
}

// EncodePair creates an int from x, y coordinates
func EncodePair(x, y int32) int {
	return int(x)*(math.MaxUint32+1) + int(y)
}

// DecodePair retrieves x, y from an encoded pair
func DecodePair(p int) (int32, int32) {
	return int32(p / (math.MaxUint32 + 1)), int32(p % (math.MaxUint32 + 1))
}

type Iterators [4]Iterator

func NewIteratorsFromString(s string, nbRecords, nbBands int, zoomMinMaxBlock [][4]int32) (*Iterators, error) {
	its := strings.Split(s, ">")
	if len(its) != 4 {
		return nil, fmt.Errorf("%s must have four level of iterations, got %d", s, len(its))
	}

	var res Iterators
	for i, it := range its {
		itSplit := strings.SplitN(it, "=", 2)
		switch itSplit[0] {
		case KEY_TILE:
			res[i] = NewTileIterator(IDX_TILE, zoomMinMaxBlock)

		case KEY_BAND, KEY_RECORD, KEY_ZOOM:
			var idx, maxV int
			switch itSplit[0] {
			case KEY_BAND:
				idx, maxV = IDX_BAND, nbBands
			case KEY_RECORD:
				idx, maxV = IDX_RECORD, nbRecords
			case KEY_ZOOM:
				idx, maxV = IDX_ZOOM, len(zoomMinMaxBlock)
			}
			if len(itSplit) == 1 || strings.Contains(itSplit[1], ":") {
				// Using range
				minV := 0
				if len(itSplit) == 2 {
					valuesS := strings.SplitN(itSplit[1], ":", 2)
					if valuesS[0] != "" { // Parse first value of the range
						nMinV, err := strconv.Atoi(valuesS[0])
						if err != nil {
							return nil, fmt.Errorf("cannot parse min value of range %s: %w", itSplit[1], err)
						}
						if nMinV > minV {
							minV = nMinV
						}
					}
					if valuesS[1] != "" { // Parse last value of the range
						nMaxV, err := strconv.Atoi(valuesS[1])
						if err != nil {
							return nil, fmt.Errorf("cannot parse max value of range %s: %w", itSplit[1], err)
						}
						if nMaxV < maxV {
							maxV = nMaxV
						}
					}
				}
				res[i] = NewRangeIterator(idx, minV, maxV)
			} else {
				// Using values
				valuesS := strings.Split(itSplit[1], ",")
				var values []int
				for _, v := range valuesS {
					v, err := strconv.Atoi(v)
					if err != nil {
						return nil, fmt.Errorf("cannot parse values of %s: %w", it, err)
					}
					if 0 <= v && v <= maxV {
						values = append(values, v)
					}
				}
				res[i] = NewValuesIterator(idx, values)
			}
		default:
			return nil, fmt.Errorf("unknown key %s: must be one of [%s, %s, %s, %s]", itSplit[0], KEY_BAND, KEY_RECORD, KEY_ZOOM, KEY_TILE)
		}
	}
	return &res, res.Check()
}

func (its Iterators) Check() error {
	defined := [4]bool{}
	for _, iter := range its {
		idx := iter.ID()
		if idx > len(defined) {
			return fmt.Errorf("Iterators.Check: unknown index %d", idx)
		}
		if defined[idx] {
			return fmt.Errorf("Iterators.Check: %s (idx=%d) is defined twice", Names[idx], idx)
		}
		if idx == IDX_TILE && !defined[IDX_ZOOM] {
			return fmt.Errorf("Iterators.Check: %s (idx=%d) cannot be defined before %s (idx=%d)", Names[IDX_TILE], IDX_TILE, Names[IDX_ZOOM], IDX_ZOOM)
		}
		defined[idx] = true
	}
	return nil
}
