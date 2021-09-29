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
			"second-dummy": {
				Type: "dummy",
			},
		},
		PprofHttpBind: "localhost:6034",
	}

	if !reflect.DeepEqual(resultingConfig, expectedConfig) {
		t.Fail()
	}
}
