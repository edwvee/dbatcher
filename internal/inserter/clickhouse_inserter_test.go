package inserter

import (
	"database/sql"
	"io/ioutil"
	"log"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/edwvee/dbatcher/internal/table"
	jsoniter "github.com/json-iterator/go"
)

const clickhouseDsnKey = "DBATCHER_TEST_CLICKHOUSE_DSN_KEY"
const tableName = "default.dbatcher_test_table"

var fieldsSlice = []string{
	`uint8Number`,
	`uint16Number`,
	`uint32Number`,
	`uint64Number`,
	`int8Number`,
	`int16Number`,
	`int32Number`,
	`int64Number`,
	`uint8String`,
	`uint16String`,
	`uint32String`,
	`uint64String`,
	`int8String`,
	`int16String`,
	`int32String`,
	`int64String`,
	`float32Number`,
	`float64Number`,
	`stringString`,
	`stringFStrinF`,
	`dateNumber`,
	`dateTimeNumber`,
	`dateString`,
	`dateTimeString`,
	`dateTime64String`,
	`enum8Number`,
	`enum16Number`,
	`enum8String`,
	`enum16String`,
}

var fields = strings.Join(fieldsSlice, ",")

var clickhouse *sql.DB

func init() {
	dsn := os.Getenv(clickhouseDsnKey)
	if dsn == "" {
		return
	}
	script, err := ioutil.ReadFile("../../scripts/clickhouse_test_table.sql")
	if err != nil {
		panic(err)
	}
	statements := strings.Split(string(script), ";")
	clickhouse, err = connectDB("clickhouse", dsn, 2)
	if err != nil {
		panic(err)
	}
	for _, statement := range statements {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		log.Print(statement)
		if _, err := clickhouse.Exec(statement); err != nil {
			panic(err)
		}
	}
}

func TestClickhouseGetTableStructure(t *testing.T) {
	dsn := os.Getenv(clickhouseDsnKey)
	if dsn == "" {
		t.SkipNow()
	}

	ins := ClickHouseInserter{}
	ins.Init(Config{
		Type:            "clickhouse",
		Dsn:             dsn,
		MaxConnections:  2,
		InsertTimeoutMs: 30000,
	})
	ts := table.NewTableSignature(tableName, fields)
	table := table.NewTable(ts)
	structure, err := ins.getTableStructure(table)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fullTypeClickhouseStructure, structure) {
		t.Errorf("table structures are not equal, want %v, got %v", fullTypeClickhouseStructure, structure)
	}
}

type clickhouseTestRowNoTime struct {
	uint8Number   uint8
	uint16Number  uint16
	uint32Number  uint32
	uint64Number  uint64
	int8Number    int8
	int16Number   int16
	int32Number   int32
	int64Number   int64
	uint8String   uint8
	uint16String  uint16
	uint32String  uint32
	uint64String  uint64
	int8String    int8
	int16String   int16
	int32String   int32
	int64String   int64
	float32Number float32
	float64Number float64
	stringString  string
	stringFStrinF string
	enum8Number   string
	enum16Number  string
	enum8String   string
	enum16String  string
}

type clickhouseTestRowTime struct {
	dateNumber       time.Time
	dateTimeNumber   time.Time
	dateString       time.Time
	dateTimeString   time.Time
	dateTime64String time.Time
}

func TestClickshouseInsert(t *testing.T) {
	dsn := os.Getenv(clickhouseDsnKey)
	if dsn == "" {
		t.SkipNow()
	}

	ins := ClickHouseInserter{}
	ins.Init(Config{
		Type:            "clickhouse",
		Dsn:             dsn,
		MaxConnections:  2,
		InsertTimeoutMs: 30000,
	})
	ts := table.NewTableSignature(tableName, fields)
	table := table.NewTable(ts)
	now := time.Now()
	rows := [][]interface{}{{
		uint8(1 << 2),                         //"uint8Number":      chUInt8,
		uint16(1 << 9),                        //"uint16Number":     chUInt16,
		uint32(1 << 17),                       //"uint32Number":     chUInt32,
		uint64(math.MaxUint64),                //"uint64Number":     chUInt64,
		int8(-1 * (1 << 2)),                   //"int8Number":       chInt8,
		int16(-1 * (1 << 9)),                  //"int16Number":      chInt16,
		int32(-1 * (1 << 17)),                 //"int32Number":      chInt32,
		int64(-1 * (1 << 62)),                 //"int64Number":      chInt64,
		uint8(1 << 2),                         //"uint8String":      chUInt8,
		uint16(1 << 9),                        //"uint16String":     chUInt16,
		uint32(1 << 17),                       //"uint32String":     chUInt32,
		uint64(math.MaxUint64),                //"uint64String":     chUInt64,
		int8(-1 * (1 << 2)),                   //"int8String":       chInt8,
		int16(-1 * (1 << 9)),                  //"int16String":      chInt16,
		int32(-1 * (1 << 17)),                 //"int32String":      chInt32,
		int64(-1 * (1 << 62)),                 //"int64String":      chInt64,
		float32(34435.353535),                 //"float32Number":    chFloat32,
		float64(34454435.353535),              //"float64Number":    chFloat64,
		"string",                              //"stringString":     chString,
		"0123456789abcdef",                    //"stringFStrinF":    chFixedString,
		int64(now.Unix()),                     //"dateNumber":       chDate,
		int64(now.Unix()),                     //"dateTimeNumber":   chDateTime,
		now.Format("2006-01-02"),              //"dateString":       chDate,
		now.Format("2006-01-02 15:04:05"),     //"dateTimeString":   chDateTime,
		now.Format("2006-01-02 15:04:05.999"), //"dateTime64String": chDateTime64,
		int16(1),                              //"enum8Number":      chEnum8,
		int16(2),                              //"enum16Number":     chEnum16,
		"a",                                   //"enum8String":      chEnum8,
		"b",                                   //"enum16String":     chEnum16,
	}}
	jsonData, err := jsoniter.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	err = table.AppendRows(jsonData)
	if err != nil {
		t.Fatal(err)
	}
	err = ins.Insert(table)
	if err != nil {
		t.Fatal(err)
	}

	target := clickhouseTestRowNoTime{}
	targetTime := clickhouseTestRowTime{}
	sql := "SELECT * FROM default.dbatcher_test_table"
	rowQuery := clickhouse.QueryRow(sql)
	err = rowQuery.Scan(
		&target.uint8Number,
		&target.uint16Number,
		&target.uint32Number,
		&target.uint64Number,
		&target.int8Number,
		&target.int16Number,
		&target.int32Number,
		&target.int64Number,
		&target.uint8String,
		&target.uint16String,
		&target.uint32String,
		&target.uint64String,
		&target.int8String,
		&target.int16String,
		&target.int32String,
		&target.int64String,
		&target.float32Number,
		&target.float64Number,
		&target.stringString,
		&target.stringFStrinF,
		&targetTime.dateNumber,
		&targetTime.dateTimeNumber,
		&targetTime.dateString,
		&targetTime.dateTimeString,
		&targetTime.dateTime64String,
		&target.enum8Number,
		&target.enum16Number,
		&target.enum8String,
		&target.enum16String,
	)
	if err != nil {
		t.Fatal(err)
	}
	expectedRow := clickhouseTestRowNoTime{
		uint8Number:   1 << 2,
		uint16Number:  1 << 9,
		uint32Number:  1 << 17,
		uint64Number:  math.MaxUint64,
		int8Number:    -1 * (1 << 2),
		int16Number:   -1 * (1 << 9),
		int32Number:   -1 * (1 << 17),
		int64Number:   -1 * (1 << 62),
		uint8String:   1 << 2,
		uint16String:  1 << 9,
		uint32String:  1 << 17,
		uint64String:  math.MaxUint64,
		int8String:    -1 * (1 << 2),
		int16String:   -1 * (1 << 9),
		int32String:   -1 * (1 << 17),
		int64String:   -1 * (1 << 62),
		float32Number: 34435.353535,
		float64Number: 34454435.353535,
		stringString:  "string",
		stringFStrinF: "0123456789abcdef",
		enum8Number:   "a",
		enum16Number:  "b",
		enum8String:   "a",
		enum16String:  "b",
	}
	if !reflect.DeepEqual(target, expectedRow) {
		t.Error("rows should be equal")
	}
	if got, want := targetTime.dateNumber.Format("2006-01-02"), now.Format("2006-01-02"); got != want {
		t.Errorf("dateNumber field failed: got %s, want %s", got, want)
	}
	if got, want := targetTime.dateTimeNumber.Format("2006-01-02 15:04:05"), now.Format("2006-01-02 15:04:05"); got != want {
		t.Errorf("dateTimeNumber field failed: got %s, want %s", got, want)
	}
	if got, want := targetTime.dateTime64String.Format("2006-01-02 15:04:05.999"), now.Format("2006-01-02 15:04:05.999"); got != want {
		t.Errorf("dateTime64String field failed: got %s, want %s", got, want)
	}
	if got, want := targetTime.dateString.Format("2006-01-02"), now.Format("2006-01-02"); got != want {
		t.Errorf("dateString field failed: got %s, want %s", got, want)
	}
	if got, want := targetTime.dateTimeString.Format("2006-01-02 15:04:05"), now.Format("2006-01-02 15:04:05"); got != want {
		t.Errorf("dateTimeString field failed: got %s, want %s", got, want)
	}
}
