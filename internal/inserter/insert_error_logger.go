package inserter

import (
	"io"
	"os"
	"sync"
	"time"

	"github.com/edwvee/dbatcher/internal/table"
	jsoniter "github.com/json-iterator/go"
)

type InsertErrorLogger struct {
	w           io.Writer
	prettyPrint bool
	mut         sync.Mutex
}

type insertErrorLoggerData struct {
	TimeStamp       int64           `json:"timestamp"`
	TimeStampString string          `json:"timestamp_string"`
	Error           string          `json:"error"`
	Table           string          `json:"table"`
	Fields          string          `json:"string"`
	Rows            [][]interface{} `json:"rows"`
}

func NewInsertErrorLoggerFromConfig(config InsertErrorLoggerConfig) (*InsertErrorLogger, error) {
	if config.Path == "" {
		return NewInsertErrorLogger(nil, false), nil
	}
	f, err := os.OpenFile(config.Path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return NewInsertErrorLogger(f, config.PrettyPrint), nil
}

func NewInsertErrorLogger(w io.Writer, prettyPrint bool) *InsertErrorLogger {
	return &InsertErrorLogger{w: w, prettyPrint: prettyPrint}
}

func (l *InsertErrorLogger) Log(insertError error, t *table.Table) error {
	if l.w == nil {
		return nil
	}

	data := l.MakeData(insertError, t)

	l.mut.Lock()
	defer l.mut.Unlock()
	encoder := jsoniter.NewEncoder(l.w)
	if l.prettyPrint {
		encoder.SetIndent("", "    ")
	}
	err := encoder.Encode(data)
	if err != nil {
		return err
	}

	return err
}

func (l *InsertErrorLogger) MakeData(insertError error, t *table.Table) insertErrorLoggerData {
	now := time.Now()
	data := insertErrorLoggerData{
		TimeStamp:       now.Unix(),
		TimeStampString: now.String(),
		Error:           insertError.Error(),
		Table:           t.GetTableName(),
		Fields:          t.GetFields(),
		Rows:            make([][]interface{}, 0, t.GetRowsLen()),
	}
	for row := t.GetNextRow(); row != nil; row = t.GetNextRow() {
		data.Rows = append(data.Rows, row)
	}

	return data
}

func (l *InsertErrorLogger) Close() error {
	if c, ok := l.w.(io.Closer); ok {
		return c.Close()
	}

	return nil
}
