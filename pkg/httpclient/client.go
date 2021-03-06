package httpclient

import (
	"fmt"
	"net/url"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"github.com/valyala/fasthttp"
)

//ClientConfig is a config for cliet
type ClientConfig struct {
	ServerAddress string
	ReadTimeout   time.Duration
	WriteTimeout  time.Duration
}

//Client is a dbatcher's HTTP client
type Client struct {
	client        *fasthttp.Client
	serverAddress string
}

//NewClient returns configured client
func NewClient(config ClientConfig) *Client {
	return &Client{
		client: &fasthttp.Client{
			ReadTimeout:              config.ReadTimeout,
			WriteTimeout:             config.WriteTimeout,
			NoDefaultUserAgentHeader: true,
		},
		serverAddress: config.ServerAddress,
	}
}

//Send sends table parameters and rows to dbatcher.
//Rows must be slice of slices of primitives like int, float or string
func (c Client) Send(table, fields string, timeoutMs, maxRows uint, sync, persist bool, rows interface{}) error {
	url := c.makeURL(table, fields, timeoutMs, maxRows, sync, persist)
	data, err := jsoniter.Marshal(rows)
	if err != nil {
		return errors.Wrap(err, "dbatcher http client")
	}

	request := fasthttp.AcquireRequest()
	response := fasthttp.AcquireResponse()
	defer func() {
		fasthttp.ReleaseRequest(request)
		fasthttp.ReleaseResponse(response)
	}()

	request.Header.SetMethod(fasthttp.MethodPost)
	request.SetRequestURI(url)
	request.SetBodyRaw(data)
	err = c.client.Do(request, response)
	if err != nil {
		return errors.Wrap(err, "dbatcher http client")
	}
	if code := response.StatusCode(); code != fasthttp.StatusOK {
		errorString := fmt.Sprintf(
			"dbatcher http client: got not 200 response: code %d, response: %s",
			code, string(response.Body()),
		)
		return errors.New(errorString)
	}

	return nil
}

func (c Client) makeURL(table, fields string, timeoutMs, maxRows uint, sync, persist bool) string {
	if sync {
		return fmt.Sprintf(
			"%s/?table=%s&fields=%s&sync=1",
			c.serverAddress, url.QueryEscape(table), url.QueryEscape(fields),
		)
	}
	persistStr := ""
	if persist {
		persistStr = "&persist=1"
	}
	return fmt.Sprintf(
		"%s/?table=%s&fields=%s&timeout_ms=%d&max_rows=%d%s",
		c.serverAddress, url.QueryEscape(table), url.QueryEscape(fields),
		timeoutMs, maxRows, persistStr,
	)
}

//Close closes connection to dbatcher
func (c *Client) Close() error {
	c.client.CloseIdleConnections()
	return nil
}
