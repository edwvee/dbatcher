package tablemanager

import (
	"errors"
)

var (
	//ErrZeroTimeoutMs means that timeout ms is 0
	ErrZeroTimeoutMs = errors.New("timeout_ms couldn't be zero")
	//ErrZeroMaxRows means that maxRows is 0
	ErrZeroMaxRows = errors.New("max_rows couldn't be zero")
	//ErrPersistNotFalse means that persist is not false (persist is yet not supported)
	ErrPersistNotFalse = errors.New("persist is not yet supported")
)

//Config has viable for TableManager fields like timeout and max rows
type Config struct {
	TimeoutMs int64
	MaxRows   int64
	Persist   bool
}

//NewConfig returns a ready to use config
func NewConfig(timeoutMs int64, maxRows int64, persist bool) Config {
	return Config{
		TimeoutMs: timeoutMs,
		MaxRows:   maxRows,
		Persist:   persist,
	}
}

//Validate checks if config is valid
func (c Config) Validate() error {
	if c.TimeoutMs == 0 {
		return ErrZeroTimeoutMs
	}
	if c.MaxRows == 0 {
		return ErrZeroMaxRows
	}
	if c.Persist {
		return ErrPersistNotFalse
	}

	return nil
}
