package inserter

import (
	"database/sql"
	"io/ioutil"
	"log"
	"math"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/edwvee/dbatcher/internal/table"
	jsoniter "github.com/json-iterator/go"
)

const mysqlDsnKey = "DBATCHER_TEST_MYSQL_DSN_KEY"
const mysqlTestTableName = "db_name.dbatcher_test_table"

var mysqlTestFieldsSlice = []string{
	"uTinyIntNumber",
	"uSmallIntNumber",
	"uIntNumber",
	"uBigIntNumber",
	"tinyIntNumber",
	"smallIntNumber",
	"intNumber",
	"bigIntNumber",
	"floatNumber",
	"doubleNumber",

	"uTinyIntString",
	"uSmallIntString",
	"uIntString",
	"uBigIntString",
	"tinyIntString",
	"smallIntString",
	"intString",
	"bigIntString",
	"floatString",
	"doubleString",

	"dateString",
	"dateTimeString",
	"timestampString",
	"char32String",
	"binary8String",
	"varchar255String",
	"textString",
	"enumString",
	"enumNumber",
}

var mysqlTestFields = strings.Join(mysqlTestFieldsSlice, ",")

var mysql *sql.DB

func init() {
	dsn := os.Getenv(mysqlDsnKey)
	if dsn == "" {
		return
	}
	script, err := ioutil.ReadFile("../../scripts/mysql_test_table.sql")
	if err != nil {
		panic(err)
	}
	statements := strings.Split(string(script), ";")
	mysql, err = connectDB("mysql", dsn, 2)
	if err != nil {
		panic(err)
	}
	for _, statement := range statements {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		log.Print(statement)
		if _, err := mysql.Exec(statement); err != nil {
			panic(err)
		}
	}
}

type mysqlTestRow struct {
	uTinyIntNumber  uint8
	uSmallIntNumber uint16
	uIntNumber      uint32
	uBigIntNumber   uint64
	tinyIntNumber   int8
	smallIntNumber  int16
	intNumber       int32
	bigIntNumber    int64
	floatNumber     float32
	doubleNumber    float64

	uTinyIntString  uint8
	uSmallIntString uint16
	uIntString      uint32
	uBigIntString   uint64
	tinyIntString   int8
	smallIntString  int16
	intString       int32
	bigIntString    int64
	floatString     float32
	doubleString    float64

	dateString       string
	dateTimeString   string
	timestampString  string
	char32String     string
	binary8String    string
	varchar255String string
	textString       string
	enumString       string
	enumNumber       string
}

func TestMysqlInsert(t *testing.T) {
	dsn := os.Getenv(mysqlDsnKey)
	if dsn == "" {
		t.SkipNow()
	}

	ins := MysqlInserter{}
	ins.Init(Config{
		Type:            "mysql",
		Dsn:             dsn,
		MaxConnections:  2,
		InsertTimeoutMs: 30000,
	})
	ts := table.NewSignature(mysqlTestTableName, mysqlTestFields)
	table := table.NewTable(ts)
	rows := [][]interface{}{{
		uint8(1 << 2),            //"uTinyIntNumber",
		uint16(1 << 9),           //"uSmallIntNumber",
		uint32(1 << 17),          //"uIntNumber",
		uint64(math.MaxUint64),   //"uBigIntNumber",
		int8(-1 * (1 << 2)),      //"tinyIntNumber",
		int16(-1 * (1 << 9)),     //"smallIntNumber",
		int32(-1 * (1 << 9)),     //"intNumber",
		int64(-1 * (1 << 62)),    //"bigIntNumber",
		float32(34435.4),         //"floatNumber",
		float64(34454435.353535), //"doubleNumber",

		strconv.FormatUint(1<<2, 10),           //"uTinyIntString",
		strconv.FormatUint(1<<9, 10),           //"uSmallIntString",
		strconv.FormatUint(1<<17, 10),          //"uIntString",
		strconv.FormatUint(math.MaxUint64, 10), //"uBigIntString",
		strconv.FormatInt(-1*(1<<2), 10),       //"tinyIntString",
		strconv.FormatInt(-1*(1<<9), 10),       //"smallIntString",
		strconv.FormatInt(-1*(1<<17), 10),      //"intString",
		strconv.FormatInt(-1*(1<<62), 10),      //"bigIntString",
		"34435.4",                              //"floatString",
		"34454435.353535",                      //"doubleString",

		"2021-09-29",          //"dateString",
		"2021-09-29 01:52:16", //"dateTimeString",
		"2021-09-29 01:52:16", //"timestampString",
		"asdf",                //"char32String",
		"01234567",            //"binary8String",
		"test_string",         //"varchar255String",
		"test_text",           //"textString",
		"ZXC",                 //"enumString",
		1,                     //"enumNumber",
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

	target := mysqlTestRow{}
	sql := "SELECT * FROM " + mysqlTestTableName
	rowQuery := mysql.QueryRow(sql)
	err = rowQuery.Scan(
		&target.uTinyIntNumber,
		&target.uSmallIntNumber,
		&target.uIntNumber,
		&target.uBigIntNumber,
		&target.tinyIntNumber,
		&target.smallIntNumber,
		&target.intNumber,
		&target.bigIntNumber,
		&target.floatNumber,
		&target.doubleNumber,

		&target.uTinyIntString,
		&target.uSmallIntString,
		&target.uIntString,
		&target.uBigIntString,
		&target.tinyIntString,
		&target.smallIntString,
		&target.intString,
		&target.bigIntString,
		&target.floatString,
		&target.doubleString,

		&target.dateString,
		&target.dateTimeString,
		&target.timestampString,
		&target.char32String,
		&target.binary8String,
		&target.varchar255String,
		&target.textString,
		&target.enumString,
		&target.enumNumber,
	)
	if err != nil {
		t.Fatal(err)
	}
	expectedRow := mysqlTestRow{
		uTinyIntNumber:  uint8(1 << 2),
		uSmallIntNumber: uint16(1 << 9),
		uIntNumber:      uint32(1 << 17),
		uBigIntNumber:   uint64(math.MaxUint64),
		tinyIntNumber:   int8(-1 * (1 << 2)),
		smallIntNumber:  int16(-1 * (1 << 9)),
		intNumber:       int32(-1 * (1 << 9)),
		bigIntNumber:    int64(-1 * (1 << 62)),
		floatNumber:     float32(34435.4),
		doubleNumber:    float64(34454435.353535),

		uTinyIntString:  1 << 2,
		uSmallIntString: 1 << 9,
		uIntString:      1 << 17,
		uBigIntString:   math.MaxUint64,
		tinyIntString:   -1 * (1 << 2),
		smallIntString:  -1 * (1 << 9),
		intString:       -1 * (1 << 17),
		bigIntString:    -1 * (1 << 62),
		floatString:     float32(34435.4),
		doubleString:    float64(34454435.353535),

		dateString:       "2021-09-29",
		dateTimeString:   "2021-09-29 01:52:16",
		timestampString:  "2021-09-29 01:52:16",
		char32String:     "asdf",
		binary8String:    "01234567",
		varchar255String: "test_string",
		textString:       "test_text",
		enumString:       "ZXC",
		enumNumber:       "ASD",
	}
	if !reflect.DeepEqual(target, expectedRow) {
		t.Error("rows should be equal")
	}
}

func TestInvalidConnectMysql(t *testing.T) {
	dsn := "gfdgfdfggfdm"
	ins := MysqlInserter{}
	err := ins.Init(Config{
		Type:            "mysql",
		Dsn:             dsn,
		MaxConnections:  2,
		InsertTimeoutMs: 30000,
	})
	t.Log(err)
	if err == nil {
		t.Error("should be an error")
	}
}
