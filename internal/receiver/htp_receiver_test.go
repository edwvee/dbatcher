package receiver

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/edwvee/dbatcher/internal/inserter"
	"github.com/edwvee/dbatcher/internal/table"
	"github.com/edwvee/dbatcher/internal/tablemanager"
	"github.com/valyala/fasthttp"
)

const bindEnvKey = "DBATCHER_HTTP_RECEIVER_TEST_BIND"

var defaultHTTPReceiverBind = "localhost:8090"

var defaultHTTPReceiverConfig Config

func init() {
	bind := os.Getenv(bindEnvKey)
	if bind != "" {
		defaultHTTPReceiverBind = bind
	}
	defaultHTTPReceiverConfig = Config{
		Type: "http",
		Bind: defaultHTTPReceiverBind,
	}
}

func TestReceive(t *testing.T) {
	rec := &HTTPReceiver{}
	errChan := make(chan error)
	inserters := map[string]inserter.Inserter{
		"dummy": &inserter.DummyInserter{},
	}
	tmh := tablemanager.NewHolder(errChan, inserters)
	if err := rec.Init(defaultHTTPReceiverConfig, errChan, tmh); err != nil {
		t.Errorf("shouldn't return error: %s", err.Error())
	}

	rec.Receive()
	defer rec.Stop()
	time.Sleep(time.Millisecond * 100)
	select {
	case <-errChan:
		t.Fatal("there shouldn't be an error. server didn't start")
	default:
	}
	code, _, err := fasthttp.Get(nil, "http://"+defaultHTTPReceiverBind+"/")
	if err != nil {
		t.Fatalf("got error trying to get: %s", err.Error())
	}
	if code == 0 {
		t.Error("response code shouldn't be 0")
	}

	recShouldntStart := &HTTPReceiver{}
	recShouldntStart.Init(defaultHTTPReceiverConfig, errChan, tmh)
	recShouldntStart.Receive()
	time.Sleep(time.Millisecond * 100)
	select {
	case <-errChan:
	default:
		t.Error("after starting second receiver there should be an error. cause it can't bind to the same address")
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

func TestHTTPReceiverByRequests(t *testing.T) {
	rec := &HTTPReceiver{}
	errChan := make(chan error)
	ins := &selfSliceInserter{}
	inserters := map[string]inserter.Inserter{
		"first": ins,
	}
	tmh := tablemanager.NewHolder(errChan, inserters)
	if err := rec.Init(defaultHTTPReceiverConfig, errChan, tmh); err != nil {
		t.Errorf("shouldn't return error: %s", err.Error())
	}
	rec.Receive()
	defer rec.Stop()
	time.Sleep(time.Millisecond * 100)
	select {
	case <-errChan:
		t.Fatal("there shouldn't be an error. server didn't start")
	default:
	}

	//wrong method
	code, _, err := fasthttp.Get(nil, "http://"+defaultHTTPReceiverBind+"/")
	if err != nil {
		t.Fatalf("got error trying to get: %s", err.Error())
	}
	if code != 405 {
		t.Errorf("code should be 405, got: %d", code)
	}

	//test invalid table
	url := fmt.Sprintf("http://%s/?table=%s&fields=%s", defaultHTTPReceiverBind, "table", "field1,,field3")
	code, _, err = fasthttp.Post(nil, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if code != 400 {
		t.Errorf("code should be 400, got: %d", code)
	}
	url = fmt.Sprintf("http://%s/?table=%s&fields=%s", defaultHTTPReceiverBind, "table", "field1,field2")

	//invalid timeout_ms
	url0 := url + "&timeout_ms=fasdf"
	code, _, err = fasthttp.Post(nil, url0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if code != 400 {
		t.Errorf("code should be 400, got: %d", code)
	}
	url += "&timeout_ms=1000"

	url0 = url + "&max_rows=dfsf"
	code, _, err = fasthttp.Post(nil, url0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if code != 400 {
		t.Errorf("code should be 400, got: %d", code)
	}
	url += "&max_rows=10"

	//yet not support persist
	url0 = url + "&persist=1"
	code, _, err = fasthttp.Post(nil, url0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if code != 400 {
		t.Errorf("code should be 400, got: %d", code)
	}

	client := fasthttp.Client{}
	request := fasthttp.AcquireRequest()
	response := fasthttp.AcquireResponse()

	//unsuccsessfull insert
	request.Header.SetMethod(fasthttp.MethodPost)
	request.SetRequestURI(url)
	request.SetBodyRaw([]byte("[[2]]"))
	err = client.Do(request, response)
	if err != nil {
		t.Errorf("shouldn't be an error: %s", err.Error())
	}
	if code := response.StatusCode(); code != 400 {
		t.Error("should be error")
	}

	//10 succesfull inserts
	request.SetBodyRaw([]byte("[[2,3]]"))
	for i := 0; i < 10; i++ {
		err = client.Do(request, response)
		if err != nil {
			t.Errorf("shouldn't be an error: %s", err.Error())
		}
		if code := response.StatusCode(); code != 200 {
			t.Error("should be error")
		}
	}
	time.Sleep(time.Millisecond * 100)
	data := ins.TakeSlice()
	if len(data) != 10 {
		t.Errorf("send 10 records, got %d", len(data))
	}

}

//freezed cause there is no way to shutdown fasthttp.Server with idle connectios yet
/*
func TestShutdown(t *testing.T) {
	errChan := make(chan error)
	tH := tablemanager.NewTableManagerHolder(errChan, nil)
	rec := &HTTPReceiver{}
	err := rec.Init(defaultHttpReceiverConfig, errChan, tH)
	if err != nil {
		t.Fatal(err)
	}
	rec.Receive()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := 0; i < 100; i++ {
		go sendRowsContiniously(ctx, errChan)
	}
	time.Sleep(time.Second)

	err = rec.Stop()
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-errChan:
		t.Fatal(err)
	default:
	}
}

func sendRowsContiniously(ctx context.Context, errChan chan error) {
	config := httpclient.ClientConfig{
		ServerAddress: "http://" + defaultHttpReceiverBind,
		ReadTimeout:   60 * time.Second,
		WriteTimeout:  60 * time.Second,
	}
	client := httpclient.NewClient(config)
	rows := [][]interface{}{{"foo", "bar"}}
main_cycle:
	for {
		err := client.Send("foo", "foo,bar", 20000, 30000, false, false, rows)
		if err != nil {
			errChan <- err
		}
		select {
		case <-ctx.Done():
			break main_cycle
		default:
		}
	}
}
*/
