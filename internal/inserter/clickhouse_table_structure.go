package inserter

import (
	"errors"
	"strconv"
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
			switch el.(type) {
			case float64:
				resEl = uint8(el.(float64))
			case string:
				preResEl, err := strconv.ParseUint(el.(string), 10, 8)
				if err != nil {
					return nil, err
				}
				resEl = uint8(preResEl)
			default:
				return nil, ErrCantParseToClickhouseType
			}
		case chUInt16:
			switch el.(type) {
			case float64:
				resEl = uint16(el.(float64))
			case string:
				preResEl, err := strconv.ParseUint(el.(string), 10, 16)
				if err != nil {
					return nil, err
				}
				resEl = uint16(preResEl)
			default:
				return nil, ErrCantParseToClickhouseType
			}
		case chUInt32:
			switch el.(type) {
			case float64:
				resEl = uint32(el.(float64))
			case string:
				preResEl, err := strconv.ParseUint(el.(string), 10, 32)
				if err != nil {
					return nil, err
				}
				resEl = uint32(preResEl)
			default:
				return nil, ErrCantParseToClickhouseType
			}
		case chUInt64:
			switch el.(type) {
			case float64:
				resEl = uint64(el.(float64))
			case string:
				preResEl, err := strconv.ParseUint(el.(string), 10, 16)
				if err != nil {
					return nil, err
				}
				resEl = preResEl
			default:
				return nil, ErrCantParseToClickhouseType
			}
		case chInt8:
			switch el.(type) {
			case float64:
				resEl = int8(el.(float64))
			case string:
				preResEl, err := strconv.ParseInt(el.(string), 10, 8)
				if err != nil {
					return nil, err
				}
				resEl = int8(preResEl)
			default:
				return nil, ErrCantParseToClickhouseType
			}
		case chInt16:
			switch el.(type) {
			case float64:
				resEl = int16(el.(float64))
			case string:
				preResEl, err := strconv.ParseInt(el.(string), 10, 16)
				if err != nil {
					return nil, err
				}
				resEl = int16(preResEl)
			default:
				return nil, ErrCantParseToClickhouseType
			}
		case chInt32:
			switch el.(type) {
			case float64:
				resEl = int32(el.(float64))
			case string:
				preResEl, err := strconv.ParseInt(el.(string), 10, 32)
				if err != nil {
					return nil, err
				}
				resEl = int32(preResEl)
			default:
				return nil, ErrCantParseToClickhouseType
			}
		case chInt64:
			switch el.(type) {
			case float64:
				resEl = int64(el.(float64))
			case string:
				preResEl, err := strconv.ParseInt(el.(string), 10, 64)
				if err != nil {
					return nil, err
				}
				resEl = int64(preResEl)
			default:
				return nil, ErrCantParseToClickhouseType
			}
		case chFloat32:
			preResEl, ok := el.(float64)
			if !ok {
				return nil, ErrCantParseToClickhouseType
			}
			resEl = float32(preResEl)
		case chFloat64:
			preResEl, ok := el.(float64)
			if !ok {
				return nil, ErrCantParseToClickhouseType
			}
			resEl = preResEl
		case chString, chFixedString:
			preResEl, ok := el.(string)
			if !ok {
				return nil, ErrCantParseToClickhouseType
			}
			resEl = preResEl
		case chDate, chDateTime:
			switch el.(type) {
			case float64:
				resEl = int64(el.(float64))
			case string:
				preResEl, err := strconv.ParseInt(el.(string), 10, 64)
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
			switch el.(type) {
			case float64:
				resEl = int64(el.(float64))
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
