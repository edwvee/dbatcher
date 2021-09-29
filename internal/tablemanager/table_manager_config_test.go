package tablemanager

import (
	"errors"
	"reflect"
	"testing"
)

func TestTableManagerConfig(t *testing.T) {
	timeoutMs := int64(1000)
	maxRows := int64(1000)
	persist := false

	config := NewTableManagerConfig(timeoutMs, maxRows, persist)
	expectedConfig := TableManagerConfig{
		TimeoutMs: timeoutMs,
		MaxRows:   maxRows,
		Persist:   persist,
	}
	if !reflect.DeepEqual(config, expectedConfig) {
		t.Errorf("expecting config %v, got config %v", config, expectedConfig)
		t.FailNow()
	}

	if err := config.Validate(); err != nil {
		t.Error(err)
	}
	config.MaxRows = 0
	if err := config.Validate(); !errors.Is(err, ErrZeroMaxRows) {
		t.Error(err)
	}
	config.MaxRows = 1000
	config.TimeoutMs = 0
	if err := config.Validate(); !errors.Is(err, ErrZeroTimeoutMs) {
		t.Error(err)
	}
	config.MaxRows = 1000
	config.TimeoutMs = 1000
	config.Persist = true
	if err := config.Validate(); !errors.Is(err, ErrPersintNotFalse) {
		t.Error(err)
	}
}
