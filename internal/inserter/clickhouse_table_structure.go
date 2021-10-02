package inserter

import (
	"encoding/json"
	"strconv"

	"github.com/pkg/errors"
)

type clickhouseType = string

const (
	chUInt8       = clickhouseType("UInt8")
	chUInt16      = clickhouseType("UInt16")
	chUInt32      = clickhouseType("UInt32")
	chUInt64      = clickhouseType("UInt64")
	chInt8        = clickhouseType("Int8")
	chInt16       = clickhouseType("Int16")
	chInt32       = clickhouseType("Int32")
	chInt64       = clickhouseType("Int64")
	chFloat32     = clickhouseType("Float32")
	chFloat64     = clickhouseType("Float64")
	chString      = clickhouseType("String")
	chFixedString = clickhouseType("FixedString")
	chDate        = clickhouseType("Date")
	chDateTime    = clickhouseType("DateTime")
	chDateTime64  = clickhouseType("DateTime64")
	chEnum8       = clickhouseType("Enum8")
	chEnum16      = clickhouseType("Enum16")
)

type clickhouseStructure map[string]clickhouseType

var ErrCantParseToClickhouseType = errors.New("can't parse clickhouse type")

func (s clickhouseStructure) ConvertJsonRow(columns []string, jsonRow []interface{}) (row []interface{}, err error) {
	row = make([]interface{}, 0, len(jsonRow))
	for i, el := range jsonRow {
		columnType := s[columns[i]]
		var resEl interface{}
		switch columnType {
		case chUInt8:
			switch el := el.(type) {
			case json.Number:
				val, err := strconv.ParseUint(string(el), 10, 8)
				if err != nil {
					return nil, errors.Wrap(err, "convert to clickhouse type")
				}
				resEl = uint8(val)
			case string:
				preResEl, err := strconv.ParseUint(el, 10, 8)
				if err != nil {
					return nil, errors.Wrap(err, "convert to clickhouse type")
				}
				resEl = uint8(preResEl)
			default:
				return nil, ErrCantParseToClickhouseType
			}
		case chUInt16:
			switch el := el.(type) {
			case json.Number:
				val, err := strconv.ParseUint(string(el), 10, 16)
				if err != nil {
					return nil, errors.Wrap(err, "convert to clickhouse type")
				}
				resEl = uint16(val)
			case string:
				preResEl, err := strconv.ParseUint(el, 10, 16)
				if err != nil {
					return nil, errors.Wrap(err, "convert to clickhouse type")
				}
				resEl = uint16(preResEl)
			default:
				return nil, ErrCantParseToClickhouseType
			}
		case chUInt32:
			switch el := el.(type) {
			case json.Number:
				val, err := strconv.ParseUint(string(el), 10, 32)
				if err != nil {
					return nil, errors.Wrap(err, "convert to clickhouse type")
				}
				resEl = uint32(val)
			case string:
				preResEl, err := strconv.ParseUint(el, 10, 32)
				if err != nil {
					return nil, errors.Wrap(err, "convert to clickhouse type")
				}
				resEl = uint32(preResEl)
			default:
				return nil, ErrCantParseToClickhouseType
			}
		case chUInt64:
			switch el := el.(type) {
			case json.Number:
				val, err := strconv.ParseUint(string(el), 10, 64)
				if err != nil {
					return nil, errors.Wrap(err, "convert to clickhouse type")
				}
				resEl = uint64(val)
			case string:
				preResEl, err := strconv.ParseUint(el, 10, 64)
				if err != nil {
					return nil, errors.Wrap(err, "convert to clickhouse type")
				}
				resEl = preResEl
			default:
				return nil, ErrCantParseToClickhouseType
			}
		case chInt8:
			switch el := el.(type) {
			case json.Number:
				val, err := el.Int64()
				if err != nil {
					return nil, errors.Wrap(err, "convert to clickhouse type")
				}
				resEl = int8(val)
			case string:
				preResEl, err := strconv.ParseInt(el, 10, 8)
				if err != nil {
					return nil, errors.Wrap(err, "convert to clickhouse type")
				}
				resEl = int8(preResEl)
			default:
				return nil, ErrCantParseToClickhouseType
			}
		case chInt16:
			switch el := el.(type) {
			case json.Number:
				val, err := el.Int64()
				if err != nil {
					return nil, errors.Wrap(err, "convert to clickhouse type")
				}
				resEl = int16(val)
			case string:
				preResEl, err := strconv.ParseInt(el, 10, 16)
				if err != nil {
					return nil, errors.Wrap(err, "convert to clickhouse type")
				}
				resEl = int16(preResEl)
			default:
				return nil, ErrCantParseToClickhouseType
			}
		case chInt32:
			switch el := el.(type) {
			case json.Number:
				val, err := el.Int64()
				if err != nil {
					return nil, errors.Wrap(err, "convert to clickhouse type")
				}
				resEl = int32(val)
			case string:
				preResEl, err := strconv.ParseInt(el, 10, 32)
				if err != nil {
					return nil, errors.Wrap(err, "convert to clickhouse type")
				}
				resEl = int32(preResEl)
			default:
				return nil, ErrCantParseToClickhouseType
			}
		case chInt64:
			switch el := el.(type) {
			case json.Number:
				val, err := el.Int64()
				if err != nil {
					return nil, errors.Wrap(err, "convert to clickhouse type")
				}
				resEl = int64(val)
			case string:
				preResEl, err := strconv.ParseInt(el, 10, 64)
				if err != nil {
					return nil, errors.Wrap(err, "convert to clickhouse type")
				}
				resEl = int64(preResEl)
			default:
				return nil, ErrCantParseToClickhouseType
			}
		case chFloat32:
			preResEl, ok := el.(json.Number)
			if !ok {
				return nil, ErrCantParseToClickhouseType
			}
			val, err := preResEl.Float64()
			if err != nil {
				return nil, errors.Wrap(err, "convert to clickhouse type")
			}
			resEl = float32(val)
		case chFloat64:
			preResEl, ok := el.(json.Number)
			if !ok {
				return nil, ErrCantParseToClickhouseType
			}
			val, err := preResEl.Float64()
			if err != nil {
				return nil, errors.Wrap(err, "convert to clickhouse type")
			}
			resEl = val
		case chString, chFixedString:
			preResEl, ok := el.(string)
			if !ok {
				return nil, ErrCantParseToClickhouseType
			}
			resEl = preResEl
		case chDate, chDateTime:
			switch el := el.(type) {
			case json.Number:
				val, err := el.Int64()
				if err != nil {
					return nil, errors.Wrap(err, "convert to clickhouse type")
				}
				resEl = int64(val)
			case string:
				preResEl, err := strconv.ParseInt(el, 10, 64)
				if err != nil {
					resEl = el
				} else {
					resEl = int64(preResEl)
				}
			default:
				return nil, ErrCantParseToClickhouseType
			}
		case chDateTime64:
			switch el.(type) {
			case string:
				resEl = el
			default:
				return nil, ErrCantParseToClickhouseType
			}
		case chEnum8, chEnum16:
			switch el := el.(type) {
			case json.Number:
				val, err := el.Int64()
				if err != nil {
					return nil, errors.Wrap(err, "convert to clickhouse type")
				}
				resEl = int16(val)
			case string:
				resEl = el
			default:
				return nil, ErrCantParseToClickhouseType
			}
		default:
			return nil, errors.New("Clickhouse: " + string(columnType) + " type not supported")
		}
		row = append(row, resEl)
	}

	return row, err
}
