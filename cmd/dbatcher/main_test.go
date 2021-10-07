package main

import (
	"os"
	"testing"
	"time"

	"github.com/valyala/fasthttp"
)

func TestDoNotFallWithExampleConfig(t *testing.T) {
	if len(os.Args) < 2 {
		os.Args = append(os.Args, "")
	}
	os.Args[1] = "../../assets/config_example.toml"
	go main()
	time.Sleep(time.Second)

	_, _, err := fasthttp.Get(nil, "http://127.0.0.1:8124/")
	if err != nil {
		t.Errorf("http receiver doesn't work: %s", err)
	}
	//Starting receivers are last step, so if receiver do receive, then the app has started.
	//If there are some other receivers, they should be checked too.
}
