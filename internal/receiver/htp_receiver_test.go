package receiver

import (
	"context"
	"testing"
	"time"

	"github.com/edwvee/dbatcher/internal/tablemanager"
	"github.com/edwvee/dbatcher/pkg/httpclient"
)

const defaultHttpReceiverBind = "localhost:8090"

var defaultHttpReceiverConfig = Config{
	Type: "http",
	Bind: defaultHttpReceiverBind,
}

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
