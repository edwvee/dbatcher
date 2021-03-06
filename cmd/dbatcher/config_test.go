package main

import (
	"reflect"
	"testing"

	"github.com/edwvee/dbatcher/internal/inserter"
	"github.com/edwvee/dbatcher/internal/receiver"
)

func TestConfig(t *testing.T) {
	resultingConfig := getConfig("../../assets/config_example.toml")

	expectedConfig := config{
		Receivers: map[string]receiver.Config{
			"first-http": {
				Type: "http",
				Bind: ":8124",
			},
		},
		Inserters: map[string]inserter.Config{
			"first-clickhouse": {
				Type:            "clickhouse",
				Dsn:             "tcp://localhost:9000?user=default",
				MaxConnections:  2,
				InsertTimeoutMs: 30000,
			},
			"second-mysql": {
				Type:            "mysql",
				Dsn:             "root:@tcp(127.0.0.1)/?charset=utf8mb4,utf8",
				MaxConnections:  2,
				InsertTimeoutMs: 30000,
			},
			"third-dummy": {
				Type: "dummy",
			},
		},
		PprofHttpBind: "localhost:6034",
		InsertErrorLogger: inserter.InsertErrorLoggerConfig{
			Path:        "error.log",
			PrettyPrint: true,
		},
	}

	if !reflect.DeepEqual(resultingConfig, expectedConfig) {
		t.Errorf("got %#v\nwant %#v", resultingConfig, expectedConfig)
		t.Fail()
	}
}
