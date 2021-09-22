package httpclient

//Send creates Client inside and sends request to dbatcher.
//Use if you need to send single request or if performance is not a bottleneck.
func Send(config ClientConfig, table, fields string, timeoutMs, maxRows uint, sync, persist bool, rows interface{}) error {
	client := NewClient(config)
	defer client.Close()

	return client.Send(table, fields, timeoutMs, maxRows, sync, persist, rows)
}
