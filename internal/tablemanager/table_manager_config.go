package tablemanager

import (
	"errors"
)

var (
	ErrZeroTimeoutMs   = errors.New("timeout_ms couldn't be zero")
	ErrZeroMaxRows     = errors.New("max_rows couldn't be zero")
	ErrPersintNotFalse = errors.New("persist is not yet supported")
)

type TableManagerConfig struct {
	TimeoutMs int64
	MaxRows   int64
	Persist   bool
}

func NewTableManagerConfig(timeoutMs int64, maxRows int64, persist bool) TableManagerConfig {
	return TableManagerConfig{
		TimeoutMs: timeoutMs,
		MaxRows:   maxRows,
		Persist:   persist,
	}
}

func (c TableManagerConfig) Validate() error {
	if c.TimeoutMs == 0 {
		return ErrZeroTimeoutMs
	}
	if c.MaxRows == 0 {
		return ErrZeroMaxRows
	}
	if c.Persist {
		return ErrPersintNotFalse
	}

	return nil
}
