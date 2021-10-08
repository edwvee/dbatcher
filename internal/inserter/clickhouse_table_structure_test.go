package inserter

import (
	"encoding/json"
	"math"
	"reflect"
	"strconv"
	"testing"
	"time"
)

var fullTypeClickhouseStructure = clickhouseStructure{
	"uint8Number":      chUInt8,
	"uint16Number":     chUInt16,
	"uint32Number":     chUInt32,
	"uint64Number":     chUInt64,
	"int8Number":       chInt8,
	"int16Number":      chInt16,
	"int32Number":      chInt32,
	"int64Number":      chInt64,
	"uint8String":      chUInt8,
	"uint16String":     chUInt16,
	"uint32String":     chUInt32,
	"uint64String":     chUInt64,
	"int8String":       chInt8,
	"int16String":      chInt16,
	"int32String":      chInt32,
	"int64String":      chInt64,
	"float32Number":    chFloat32,
	"float64Number":    chFloat64,
	"stringString":     chString,
	"stringFStrinF":    chFixedString,
	"dateNumber":       chDate,
	"dateTimeNumber":   chDateTime,
	"dateString":       chDate,
	"dateTimeString":   chDateTime,
	"dateTime64String": chDateTime64,
	"enum8Number":      chEnum8,
	"enum16Number":     chEnum16,
	"enum8String":      chEnum8,
	"enum16String":     chEnum16,
}

func TestClickhouseTableStructureConvertJsonRowPositive(t *testing.T) {
	//gotta catch'em all
	columns := []string{
		"uint8Number",
		"uint16Number",
		"uint32Number",
		"uint64Number",
		"int8Number",
		"int16Number",
		"int32Number",
		"int64Number",
		"uint8String",
		"uint16String",
		"uint32String",
		"uint64String",
		"int8String",
		"int16String",
		"int32String",
		"int64String",
		"float32Number",
		"float64Number",
		"stringString",
		"stringFStrinF",
		"dateNumber",
		"dateTimeNumber",
		"dateString",
		"dateTimeString",
		"dateTime64String",
		"enum8Number",
		"enum16Number",
		"enum8String",
		"enum16String",
	}
	row := []interface{}{
		json.Number(strconv.FormatUint(1<<2, 10)),           //"uint8Number":      chUInt8,
		json.Number(strconv.FormatUint(1<<9, 10)),           //"uint16Number":     chUInt16,
		json.Number(strconv.FormatUint(1<<17, 10)),          //"uint32Number":     chUInt32,
		json.Number(strconv.FormatUint(math.MaxUint64, 10)), //"uint64Number":     chUInt64,
		json.Number(strconv.FormatInt(-1*(1<<2), 10)),       //"int8Number":       chInt8,
		json.Number(strconv.FormatInt(-1*(1<<9), 10)),       //"int16Number":      chInt16,
		json.Number(strconv.FormatInt(-1*(1<<17), 10)),      //"int32Number":      chInt32,
		json.Number(strconv.FormatInt(-1*(1<<62), 10)),      //"int64Number":      chInt64,
		strconv.FormatUint(1<<2, 10),                        //"uint8String":      chUInt8,
		strconv.FormatUint(1<<9, 10),                        //"uint16String":     chUInt16,
		strconv.FormatUint(1<<17, 10),                       //"uint32String":     chUInt32,
		strconv.FormatUint(math.MaxUint64, 10),              //"uint64String":     chUInt64,
		strconv.FormatInt(-1*(1<<2), 10),                    //"int8String":       chInt8,
		strconv.FormatInt(-1*(1<<9), 10),                    //"int16String":      chInt16,
		strconv.FormatInt(-1*(1<<17), 10),                   //"int32String":      chInt32,
		strconv.FormatInt(-1*(1<<62), 10),                   //"int64String":      chInt64,
		json.Number("34435.353535"),                         //"float32Number":    chFloat32,
		json.Number("34454435.353535"),                      //"float64Number":    chFloat64,
		"string",                                            //"stringString":     chString,
		"fixedString",                                       //"stringFStrinF":    chFixedString,
		json.Number("1632949379"),                           //"dateNumber":       chDate,
		"1632949379",                                        //"dateTimeNumber":   chDateTime,
		"2021-09-29",                                        //"dateString":       chDate,
		"2021-09-29 01:52:16",                               //"dateTimeString":   chDateTime,
		"2021-09-29 01:52:16.999",                           //"dateTime64String": chDateTime64,
		json.Number("1"),                                    //"enum8Number":      chEnum8,
		json.Number("1000"),                                 //"enum16Number":     chEnum16,
		"1",                                                 //"enum8String":      chEnum8,
		"1000",                                              //"enum16String":     chEnum16,
	}
	dt64, err := time.ParseInLocation("2006-01-02 15:04:05.999", "2021-09-29 01:52:16.999", time.Local)
	if err != nil {
		t.Fatal(err)
	}
	expectedRow := []interface{}{
		uint8(1 << 2),            //"uint8Number":      chUInt8,
		uint16(1 << 9),           //"uint16Number":     chUInt16,
		uint32(1 << 17),          //"uint32Number":     chUInt32,
		uint64(math.MaxUint64),   //"uint64Number":     chUInt64,
		int8(-1 * (1 << 2)),      //"int8Number":       chInt8,
		int16(-1 * (1 << 9)),     //"int16Number":      chInt16,
		int32(-1 * (1 << 17)),    //"int32Number":      chInt32,
		int64(-1 * (1 << 62)),    //"int64Number":      chInt64,
		uint8(1 << 2),            //"uint8String":      chUInt8,
		uint16(1 << 9),           //"uint16String":     chUInt16,
		uint32(1 << 17),          //"uint32String":     chUInt32,
		uint64(math.MaxUint64),   //"uint64String":     chUInt64,
		int8(-1 * (1 << 2)),      //"int8String":       chInt8,
		int16(-1 * (1 << 9)),     //"int16String":      chInt16,
		int32(-1 * (1 << 17)),    //"int32String":      chInt32,
		int64(-1 * (1 << 62)),    //"int64String":      chInt64,
		float32(34435.353535),    //"float32Number":    chFloat32,
		float64(34454435.353535), //"float64Number":    chFloat64,
		"string",                 //"stringString":     chString,
		"fixedString",            //"stringFStrinF":    chFixedString,
		int64(1632949379),        //"dateNumber":       chDate,
		int64(1632949379),        //"dateTimeNumber":   chDateTime,
		"2021-09-29",             //"dateString":       chDate,
		"2021-09-29 01:52:16",    //"dateTimeString":   chDateTime,
		dt64,                     //"dateTime64String": chDateTime64,
		int8(1),                  //"enum8Number":      chEnum8,
		int16(1000),              //"enum16Number":     chEnum16,
		"1",                      //"enum8String":      chEnum8,
		"1000",                   //"enum16String":     chEnum16,
	}

	resultRow, err := fullTypeClickhouseStructure.ConvertJSONRow(columns, row)
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	if !reflect.DeepEqual(expectedRow, resultRow) {
		t.Fail()
	}
}

func TestClickhouseTableStructureConvertJsonRowNegative(t *testing.T) {
	invalidCases := map[string]interface{}{
		"uint8Number":      json.Number("asd"),
		"uint16Number":     json.Number("asd"),
		"uint32Number":     json.Number("asd"),
		"uint64Number":     json.Number("asd"),
		"int8Number":       json.Number("asd"),
		"int16Number":      json.Number("asd"),
		"int32Number":      json.Number("asd"),
		"int64Number":      json.Number("asd"),
		"uint8String":      "asd",
		"uint16String":     "asd",
		"uint32String":     "asd",
		"uint64String":     "asd",
		"int8String":       "asd",
		"int16String":      "asd",
		"int32String":      "asd",
		"int64String":      "asd",
		"float32Number":    json.Number("asd"),
		"float64Number":    json.Number("asd"),
		"stringString":     json.Number("1"),
		"chFixedString":    json.Number("1"),
		"dateNumber":       json.Number("qwe"),
		"dateTimeNumber":   json.Number("qwe"),
		"dateString":       []interface{}{},
		"dateTimeString":   []interface{}{},
		"dateTime64String": json.Number("2323.434"),
		"enum8Number":      json.Number("1ss"),
		"enum16Number":     json.Number("1ss"),
		"enum8String":      []interface{}{},
		"enum16String":     []interface{}{},
	}
	for column, value := range invalidCases {
		columns := []string{column}
		row := []interface{}{value}
		resultRow, err := fullTypeClickhouseStructure.ConvertJSONRow(columns, row)
		if resultRow != nil {
			t.Fatalf("result row should be nill; column %s, value %x", column, value)
		}
		if err == nil {
			t.Fatalf("err shouldn't be nill; column %s, value %x", column, value)
		}
	}
	additionalCases := map[string]interface{}{
		"uint8Number":      []interface{}{},
		"uint16Number":     []interface{}{},
		"uint32Number":     []interface{}{},
		"uint64Number":     []interface{}{},
		"int8Number":       []interface{}{},
		"int16Number":      []interface{}{},
		"int32Number":      []interface{}{},
		"int64Number":      []interface{}{},
		"float32Number":    []interface{}{},
		"float64Number":    []interface{}{},
		"dateTime64String": "2323.434",
	}
	for column, value := range additionalCases {
		columns := []string{column}
		row := []interface{}{value}
		resultRow, err := fullTypeClickhouseStructure.ConvertJSONRow(columns, row)
		if resultRow != nil {
			t.Fatalf("result row should be nill; column %s, value %x", column, value)
		}
		if err == nil {
			t.Fatalf("err shouldn't be nill; column %s, value %x", column, value)
		}
	}
}
