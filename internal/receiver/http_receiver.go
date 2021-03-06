package receiver

import (
	"errors"
	"time"

	"github.com/edwvee/dbatcher/internal/table"
	"github.com/edwvee/dbatcher/internal/tablemanager"
	"github.com/valyala/fasthttp"
)

const maxShutdownTime = 2 * time.Second

//ErrDidntShutdownInTime means that HTTPReceiver didn't process all requests and
//closed all connections in time
var ErrDidntShutdownInTime = errors.New("HTTPReceiver: server didn't shutdown in time")

//HTTPReceiver receives data via HTTP
type HTTPReceiver struct {
	bind     string
	server   *fasthttp.Server
	errChan  chan error
	tMHolder *tablemanager.Holder
}

//Init configures HTTPReceiver
func (r *HTTPReceiver) Init(config Config, errChan chan error, tMHolder *tablemanager.Holder) error {
	r.bind = config.Bind
	r.errChan = errChan
	r.tMHolder = tMHolder
	r.server = &fasthttp.Server{
		Handler:               r.handle,
		CloseOnShutdown:       true,
		NoDefaultServerHeader: true,
		NoDefaultContentType:  true,
		NoDefaultDate:         true,
		ReadTimeout:           10 * time.Second,
		IdleTimeout:           300 * time.Second,
	}

	return nil
}

//Receive starts goroutine with a listening HTTP server
func (r *HTTPReceiver) Receive() {
	go r.receive()
}

func (r *HTTPReceiver) receive() {
	err := r.server.ListenAndServe(r.bind)
	if err != nil {
		r.errChan <- err
	}
}

func (r HTTPReceiver) handle(ctx *fasthttp.RequestCtx) {
	if !ctx.IsPost() {
		ctx.Error("HTTP method should be POST", 405)
		return
	}

	args := ctx.QueryArgs()

	t := string(args.Peek("table"))
	f := string(args.Peek("fields"))
	ts := table.NewSignature(t, f)
	if err := ts.Validate(); err != nil {
		ctx.Error(err.Error(), 400)
		return
	}

	tmc := tablemanager.Config{}
	sync := args.GetBool("sync")
	if !sync {
		timeoutMs, err := args.GetUint("timeout_ms")
		if err != nil {
			ctx.Error("timeout_ms: "+err.Error(), 400)
			return
		}
		maxRows, err := args.GetUint("max_rows")
		if err != nil {
			ctx.Error("max_rows: "+err.Error(), 400)
			return
		}
		persist := args.GetBool("persist")
		tmc = tablemanager.NewConfig(
			int64(timeoutMs), int64(maxRows), persist,
		)
		if err := tmc.Validate(); err != nil {
			ctx.Error(err.Error(), 400)
			return
		}
	}

	rowsData := ctx.PostBody()

	if err := r.tMHolder.Append(&ts, tmc, sync, rowsData); err != nil {
		ctx.Error(err.Error(), 400)
	}
}

//Stop wait's for request to be processed, stops listening,
//should close idle connetions (but this doesn't work yet)
func (r *HTTPReceiver) Stop() (err error) {
	timer := time.NewTimer(maxShutdownTime)
	shutdownErr := make(chan error)
	go func() {
		shutdownErr <- r.server.Shutdown()
	}()
	select {
	case <-timer.C:
		err = ErrDidntShutdownInTime
	case err = <-shutdownErr:
	}

	return
}
