package receiver

type Config struct {
	Type string `toml:"type"`
	Bind string `toml:"bind"`
}
