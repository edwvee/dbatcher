package main

import (
	"github.com/edwvee/dbatcher/internal/inserter"
	"github.com/edwvee/dbatcher/internal/receiver"
)

type config struct {
	Receivers     map[string]receiver.Config `toml:"receivers"`
	Inserters     map[string]inserter.Config `toml:"inserters"`
	PprofHttpBind string                     `toml:"pprof_http_bind"`
}
