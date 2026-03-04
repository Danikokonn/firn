package polars

/*
#include "firn.h"
*/
import "C"
import (
	"fmt"
	"sort"
	"unsafe"
)

// Series holds typed array data for creating a DataFrame column.
type Series struct {
	name    string
	dtype   int32
	dataPtr unsafe.Pointer // Points into the backing array of goData (or rawStrs for strings)
	length  int
	goData  interface{} // Keeps the original Go slice alive during FFI calls
	rawStrs []C.RawStr  // Alive for string series; dataPtr points into this slice
	err     error       // Non-nil if the data type is unsupported
}

// NewSeries creates a Series from a named Go slice.
//
// Supported element types:
//
//	[]int, []int8, []int16, []int32, []int64,
//	[]uint8, []uint16, []uint32, []uint64,
//	[]float32, []float64, []string, []bool
func NewSeries(name string, data interface{}) *Series {
	s := &Series{name: name}

	switch v := data.(type) {
	case []int8:
		s.dtype = C.SERIES_DTYPE_INT8
		s.length = len(v)
		s.goData = v
		if len(v) > 0 {
			s.dataPtr = unsafe.Pointer(&v[0])
		}
	case []int16:
		s.dtype = C.SERIES_DTYPE_INT16
		s.length = len(v)
		s.goData = v
		if len(v) > 0 {
			s.dataPtr = unsafe.Pointer(&v[0])
		}
	case []int32:
		s.dtype = C.SERIES_DTYPE_INT32
		s.length = len(v)
		s.goData = v
		if len(v) > 0 {
			s.dataPtr = unsafe.Pointer(&v[0])
		}
	case []int64:
		s.dtype = C.SERIES_DTYPE_INT64
		s.length = len(v)
		s.goData = v
		if len(v) > 0 {
			s.dataPtr = unsafe.Pointer(&v[0])
		}
	case []int:
		// Go int is platform-size; convert to int64 for a stable C type.
		converted := make([]int64, len(v))
		for i, x := range v {
			converted[i] = int64(x)
		}
		s.dtype = C.SERIES_DTYPE_INT64
		s.length = len(converted)
		s.goData = converted
		if len(converted) > 0 {
			s.dataPtr = unsafe.Pointer(&converted[0])
		}
	case []uint8:
		s.dtype = C.SERIES_DTYPE_UINT8
		s.length = len(v)
		s.goData = v
		if len(v) > 0 {
			s.dataPtr = unsafe.Pointer(&v[0])
		}
	case []uint16:
		s.dtype = C.SERIES_DTYPE_UINT16
		s.length = len(v)
		s.goData = v
		if len(v) > 0 {
			s.dataPtr = unsafe.Pointer(&v[0])
		}
	case []uint32:
		s.dtype = C.SERIES_DTYPE_UINT32
		s.length = len(v)
		s.goData = v
		if len(v) > 0 {
			s.dataPtr = unsafe.Pointer(&v[0])
		}
	case []uint64:
		s.dtype = C.SERIES_DTYPE_UINT64
		s.length = len(v)
		s.goData = v
		if len(v) > 0 {
			s.dataPtr = unsafe.Pointer(&v[0])
		}
	case []float32:
		s.dtype = C.SERIES_DTYPE_FLOAT32
		s.length = len(v)
		s.goData = v
		if len(v) > 0 {
			s.dataPtr = unsafe.Pointer(&v[0])
		}
	case []float64:
		s.dtype = C.SERIES_DTYPE_FLOAT64
		s.length = len(v)
		s.goData = v
		if len(v) > 0 {
			s.dataPtr = unsafe.Pointer(&v[0])
		}
	case []string:
		// Build a RawStr array that points into the Go string backing arrays.
		rawStrs := make([]C.RawStr, len(v))
		for i, str := range v {
			rawStrs[i] = makeRawStr(str)
		}
		s.dtype = C.SERIES_DTYPE_STRING
		s.length = len(v)
		s.goData = v       // keeps original strings alive
		s.rawStrs = rawStrs // keeps RawStr array alive
		if len(rawStrs) > 0 {
			s.dataPtr = unsafe.Pointer(&rawStrs[0])
		}
	case []bool:
		s.dtype = C.SERIES_DTYPE_BOOL
		s.length = len(v)
		s.goData = v
		if len(v) > 0 {
			s.dataPtr = unsafe.Pointer(&v[0])
		}
	default:
		s.err = fmt.Errorf("NewSeries: unsupported data type %T", data)
	}

	return s
}

// FromMap creates a DataFrame from a map of column name → slice.
// Column order is sorted alphabetically for reproducibility.
// The map values must be slices of types supported by NewSeries.
func FromMap(data map[string]interface{}) *DataFrame {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	series := make([]*Series, len(keys))
	for i, k := range keys {
		series[i] = NewSeries(k, data[k])
	}
	return newDataFrameFromSeries(series)
}

// newDataFrameFromSeries creates a DataFrame from a slice of Series.
// Used by both NewDataFrame (variadic) and FromMap.
func newDataFrameFromSeries(series []*Series) *DataFrame {
	// Validate all series before touching FFI.
	for _, s := range series {
		if s.err != nil {
			return &DataFrame{
				handle: C.PolarsHandle{handle: 0, context_type: 0},
				operations: []Operation{errOpf("NewDataFrame: %v", s.err)},
			}
		}
	}

	op := Operation{
		opcode: OpNewFromSeries,
		args: func() unsafe.Pointer {
			// Build the C SeriesData array.  The series slice (and all its
			// backing data) is captured by this closure and therefore kept
			// alive for the duration of the FFI call.
			cSeries := make([]C.SeriesData, len(series))
			for i, s := range series {
				cSeries[i] = C.SeriesData{
					name:  makeRawStr(s.name),
					dtype: C.int32_t(s.dtype),
					data:  C.uintptr_t(uintptr(s.dataPtr)),
					len:   C.size_t(s.length),
				}
			}
			return unsafe.Pointer(&C.NewFromSeriesArgs{
				series: &cSeries[0],
				count:  C.size_t(len(cSeries)),
			})
		},
	}

	return &DataFrame{
		handle:     C.PolarsHandle{handle: C.uintptr_t(0), context_type: C.uint32_t(0)},
		operations: []Operation{op},
	}
}
