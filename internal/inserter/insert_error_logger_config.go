package inserter

type InsertErrorLoggerConfig struct {
	Path        string `toml:"path"`
	PrettyPrint bool   `toml:"pretty_print"`
}
