package main

import (
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/edwvee/dbatcher/internal/inserter"
	"github.com/edwvee/dbatcher/internal/receiver"
	"github.com/edwvee/dbatcher/internal/tablemanager"
)

func main() {
	//TODO: use flags
	var c config
	configPath := "config.toml"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}
	_, err := toml.DecodeFile(configPath, &c)
	if err != nil {
		log.Fatal(err)
	}

	if c.PprofHttpBind != "" {
		go http.ListenAndServe(c.PprofHttpBind, nil)
	}

	inserters := map[string]inserter.Inserter{}
	for name, config := range c.Inserters {
		log.Printf("creating inserter %s", name)

		var ins inserter.Inserter
		switch config.Type {
		case "clickhouse":
			ins = &inserter.ClickHouseInserter{}
		case "dummy":
			ins = &inserter.DummyInserter{}
		default:
			log.Fatal("no such inserter")
		}
		if err := ins.Init(config); err != nil {
			log.Fatal(err)
		}
		inserters[name] = ins
	}

	errChan := make(chan error)
	tableManagerHolder := tablemanager.NewTableManagerHolder(errChan, inserters)

	receivers := map[string]receiver.Receiver{}
	for name, config := range c.Receivers {
		log.Printf("creating receiver %s", name)

		var rec receiver.Receiver
		switch config.Type {
		case "http":
			rec = &receiver.HTTPReceiver{}
		default:
			log.Fatal("no such receiver")
		}
		if err := rec.Init(config, errChan, tableManagerHolder); err != nil {
			log.Fatal(err)
		}
		rec.Receive()
		receivers[name] = rec
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)
	select {
	case x := <-interrupt:
		log.Printf("received a signal: %s", x.String())
	case err := <-errChan:
		log.Printf("fatal error: %s", err.Error())
	}

	for name, rec := range receivers {
		log.Printf("stoping receiver %s", name)
		err = rec.Stop()
		if err != nil {
			log.Println(err)
		}
	}

	managerErrors := tableManagerHolder.StopTableManagers()
	for _, err := range managerErrors {
		log.Println(err)
	}
}
