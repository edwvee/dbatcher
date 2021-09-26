package httpclient

import (
	"fmt"
	"net/url"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"github.com/valyala/fasthttp"
)

type ClientConfig struct {
	ServerAddress string
	ReadTimeout   time.Duration
	WriteTimeout  time.Duration
}

type Client struct {
	client        *fasthttp.Client
	serverAddress string
}

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

//TODO: full documentation
//Send
//rows must be slice of slices of primitives like int,float or string;
//if int is to big - better convert it to string
func (c Client) Send(table, fields string, timeoutMs, maxRows uint, sync, persist bool, rows interface{}) error {
	url := c.MakeUrl(table, fields, timeoutMs, maxRows, sync, persist)
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

func (c Client) MakeUrl(table, fields string, timeoutMs, maxRows uint, sync, persist bool) string {
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
		"%s/?table=%s&fields=%s&max_rows=%d&timeout_ms=%d%s",
		c.serverAddress, url.QueryEscape(table), url.QueryEscape(fields),
		maxRows, timeoutMs, persistStr,
	)

}

func (c *Client) Close() error {
	c.client.CloseIdleConnections()
	return nil
}
