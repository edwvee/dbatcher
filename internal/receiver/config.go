package receiver

//Config is a config for a receiver
type Config struct {
	Type string `toml:"type"`
	Bind string `toml:"bind"`
}
