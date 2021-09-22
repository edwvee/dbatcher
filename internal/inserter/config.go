package inserter

type Config struct {
	Type            string `toml:"type"`
	Dsn             string `toml:"dsn"`
	MaxConnections  int    `toml:"max_connections"`
	InsertTimeoutMs int    `toml:"insert_timeout_ms"`
}
