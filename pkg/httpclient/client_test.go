package httpclient

import (
	"sync"
	"testing"
	"time"

	"github.com/edwvee/dbatcher/internal/inserter"
	"github.com/edwvee/dbatcher/internal/receiver"
	"github.com/edwvee/dbatcher/internal/table"
	"github.com/edwvee/dbatcher/internal/tablemanager"
)

func TestNewClient(t *testing.T) {
	config := ClientConfig{
		ServerAddress: "http://127.0.0.1:8124",
		ReadTimeout:   5 * time.Second,
		WriteTimeout:  5 * time.Second,
	}
	client := NewClient(config)
	if client.serverAddress != config.ServerAddress {
		t.Errorf("server address doesn't much given")
	}
	if client.client.ReadTimeout != config.ReadTimeout {
		t.Errorf("read timeout doesn't match")
	}
	if client.client.WriteTimeout != config.WriteTimeout {
		t.Errorf("write timeout doesn't match")
	}
}

func TestMakeUrl(t *testing.T) {
	config := ClientConfig{
		ServerAddress: "http://127.0.0.1:8124",
		ReadTimeout:   5 * time.Second,
		WriteTimeout:  5 * time.Second,
	}
	client := NewClient(config)
	url := client.makeURL("database.table", "field1,field2", 1, 2, false, true)
	wantURL := "http://127.0.0.1:8124/?table=database.table&fields=field1%2Cfield2&timeout_ms=1&max_rows=2&persist=1"
	if url != wantURL {
		t.Errorf("want url: %s ; got %s", wantURL, url)
	}

	url = client.makeURL("database.table", "field1,field2", 1, 2, true, true)
	wantURL = "http://127.0.0.1:8124/?table=database.table&fields=field1%2Cfield2&sync=1"
	if url != wantURL {
		t.Errorf("want url: %s ; got %s", wantURL, url)
	}
}

type selfSliceInserter struct {
	data    []interface{}
	dataMut sync.Mutex
}

func (si *selfSliceInserter) Init(c inserter.Config) error {
	si.data = []interface{}{}
	return nil
}

func (si *selfSliceInserter) Insert(t *table.Table) error {
	si.dataMut.Lock()
	for row := t.GetNextRow(); row != nil; row = t.GetNextRow() {
		si.data = append(si.data, row)
	}
	si.dataMut.Unlock()

	return nil
}

func (si *selfSliceInserter) TakeSlice() []interface{} {
	si.dataMut.Lock()
	defer si.dataMut.Unlock()
	res := si.data
	si.data = nil

	return res
}

func TestSend(t *testing.T) {
	bind := "127.0.0.1:9090"
	ins := &selfSliceInserter{}
	ins.Init(inserter.Config{})
	inserters := map[string]inserter.Inserter{"first": ins}
	errChan := make(chan error)
	logger := inserter.NewInsertErrorLogger(nil, false)
	tmh := tablemanager.NewHolder(errChan, inserters, logger)
	rec := &receiver.HTTPReceiver{}
	if err := rec.Init(receiver.Config{Bind: bind}, errChan, tmh); err != nil {
		t.Fatal(err)
	}
	rec.Receive()
	time.Sleep(time.Second)

	config := ClientConfig{
		ServerAddress: "http://" + bind,
		ReadTimeout:   5 * time.Second,
		WriteTimeout:  5 * time.Second,
	}
	err := Send(config, "table", "field1, field2", 10, 10, false, false, [][]interface{}{{"1", "2"}})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond * 100)
	data := ins.TakeSlice()
	if len(data) != 1 {
		t.Fatal("didn't insert")
	}
	err = Send(config, "table", "field1, field2", 10, 10, false, true, [][]interface{}{{"1", "2"}})
	t.Log(err)
	if err == nil {
		t.Fatal("should get error if tried to use persist")
	}

	config = ClientConfig{
		ServerAddress: "ftp://" + bind,
		ReadTimeout:   0,
		WriteTimeout:  0,
	}
	err = Send(config, "table", "field1, field2", 10, 10, false, true, [][]interface{}{{"1", "2"}})
	t.Log(err)
	if err == nil {
		t.Error("should be an error")
	}
}

//bencmarks in below are for running dbatcher instance
func BenchmarkSingleRequestsKeepAlive(b *testing.B) {
	rows := [][]interface{}{
		{
			0,
			"htp://site.example/path0/path1/path2?param0=value0&param1=value1&param3=value3",
			"htp://site.example/path0/path1?param0=value0&param1=value1&param3=value3",
			666666,
			666,
		},
	}

	config := ClientConfig{
		"http://127.0.0.1:8124", 2 * time.Second, 2 * time.Second,
	}
	client := NewClient(config)
	for i := 0; i < b.N; i++ {
		rows[0][0] = time.Now().Format("2006-01-02 15:04:05.999")
		err := client.Send(
			"`visited_url`", "dt,url, sourse_url, response_time_ms, found_urls",
			100000, 10000, false, false, rows,
		)
		if err != nil {
			b.Error(err)
			b.FailNow()
		}
	}
}

func BenchmarkSingleRequestsNoKeepAlive(b *testing.B) {
	rows := [][]interface{}{
		{
			0,
			"htp://site.example/path0/path1/path2?param0=value0&param1=value1&param3=value3",
			"htp://site.example/path0/path1?param0=value0&param1=value1&param3=value3",
			666666,
			666,
			"a",
		},
	}

	for i := 0; i < b.N; i++ {
		config := ClientConfig{
			"http://127.0.0.1:8124", 2 * time.Second, 2 * time.Second,
		}
		rows[0][0] = time.Now().Format("2006-01-02 15:04:05.999")
		err := Send(
			config,
			"`visited_url`", "dt,url, sourse_url, response_time_ms, found_urls, shit",
			3000, 10000, false, false, rows,
		)
		if err != nil {
			b.Error(err)
			b.FailNow()
		}
	}
}

func BenchmarkTimeNow(b *testing.B) {
	t := time.Now()
	for i := 0; i < b.N; i++ {
		t = time.Now()
	}
	b.Log(t)
}
