/*
Copyright 2025 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package grpc

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
<<<<<<< HEAD
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
=======

	"github.com/stretchr/testify/assert"
>>>>>>> upstream/release-1.18
	"google.golang.org/grpc"

	commonv1 "github.com/dapr/dapr/pkg/proto/common/v1"
	rtv1 "github.com/dapr/dapr/pkg/proto/runtime/v1"
	"github.com/dapr/dapr/tests/integration/framework"
	procdaprd "github.com/dapr/dapr/tests/integration/framework/process/daprd"
	procgrpc "github.com/dapr/dapr/tests/integration/framework/process/grpc"
	"github.com/dapr/dapr/tests/integration/framework/process/grpc/app"
	"github.com/dapr/dapr/tests/integration/suite"
)

func init() {
	suite.Register(new(connpool))
}

<<<<<<< HEAD
// connpool verifies pooled-connection semantics against a server that
// advertises an HTTP/2 stream limit. The app enforces
// MaxConcurrentStreams=100 while 150 concurrent InvokeService calls flow
// through daprd. With the pool's shared-ref cap above the server limit,
// excess calls queue at the transport behind the app's advertised limit
// rather than fanning out onto extra connections to circumvent it: all
// calls complete, and the app never observes more in-flight work than it
// asked for. (The pre-raise pool opened a second connection at ref 101,
// silently exceeding the app's declared concurrency.)
type connpool struct {
	daprd       *procdaprd.Daprd
	maxInflight *atomic.Int32
}

const (
	numConcurrentPoolStreams = 150
	appMaxConcurrentStreams  = 100
)

func (c *connpool) Setup(t *testing.T) []framework.Option {
	var inflight atomic.Int32
	var maxInflight atomic.Int32
=======
// connpool verifies that gRPC connection pooling allows more than 100
// concurrent streams from daprd to the app via the InvokeService path.
// The app server enforces MaxConcurrentStreams=100, so without connection
// pooling only 100 simultaneous streams can exist on a single connection.
// This test sends 150 concurrent InvokeService calls through daprd, each of
// which creates a stream on a daprd->app connection. The app handler blocks
// until all 150 are in-flight simultaneously, proving that the pool opened
// additional connections to accommodate them.
type connpool struct {
	daprd      *procdaprd.Daprd
	allArrived chan struct{}
}

const numConcurrentPoolStreams = 150

func (c *connpool) Setup(t *testing.T) []framework.Option {
	c.allArrived = make(chan struct{})
	var once sync.Once
	var inflight atomic.Int32
>>>>>>> upstream/release-1.18

	onInvoke := func(ctx context.Context, in *commonv1.InvokeRequest) (*commonv1.InvokeResponse, error) {
		n := inflight.Add(1)
		defer inflight.Add(-1)
<<<<<<< HEAD
		for {
			seen := maxInflight.Load()
			if n <= seen || maxInflight.CompareAndSwap(seen, n) {
				break
			}
		}

		// Hold the stream briefly so the concurrent callers overlap and
		// the transport-level queueing is actually exercised.
		select {
		case <-time.After(100 * time.Millisecond):
=======

		if int(n) >= numConcurrentPoolStreams {
			once.Do(func() { close(c.allArrived) })
		}

		// Block until all concurrent requests have arrived, proving that
		// >100 streams are active simultaneously across pooled connections.
		select {
		case <-c.allArrived:
>>>>>>> upstream/release-1.18
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		return new(commonv1.InvokeResponse), nil
	}

<<<<<<< HEAD
	c.maxInflight = &maxInflight

	srv := app.New(t,
		app.WithOnInvokeFn(onInvoke),
		// The app declares its concurrency appetite; the pool must respect
		// it, not defeat it with extra connections.
		app.WithGRPCOptions(procgrpc.WithServerOption(
			func(*testing.T, context.Context) grpc.ServerOption {
				return grpc.MaxConcurrentStreams(appMaxConcurrentStreams)
=======
	srv := app.New(t,
		app.WithOnInvokeFn(onInvoke),
		// Enforce the 100-stream-per-connection limit so that the pool must
		// open a second connection to handle >100 concurrent streams.
		app.WithGRPCOptions(procgrpc.WithServerOption(
			func(*testing.T, context.Context) grpc.ServerOption {
				return grpc.MaxConcurrentStreams(100)
>>>>>>> upstream/release-1.18
			},
		)),
	)
	c.daprd = procdaprd.New(t,
		procdaprd.WithAppProtocol("grpc"),
		procdaprd.WithAppPort(srv.Port(t)),
	)

	return []framework.Option{
		framework.WithProcesses(srv, c.daprd),
	}
}

func (c *connpool) Run(t *testing.T, ctx context.Context) {
	c.daprd.WaitUntilRunning(t, ctx)

	conn := c.daprd.GRPCConn(t, ctx)
	client := rtv1.NewDaprClient(conn)

	var wg sync.WaitGroup
	errs := make([]error, numConcurrentPoolStreams)
	for i := range numConcurrentPoolStreams {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = client.InvokeService(ctx, &rtv1.InvokeServiceRequest{
				Id: c.daprd.AppID(),
				Message: &commonv1.InvokeRequest{
					Method:        "pool-test",
					HttpExtension: &commonv1.HTTPExtension{Verb: commonv1.HTTPExtension_POST},
				},
			})
		}(i)
	}

	wg.Wait()

	for i, err := range errs {
<<<<<<< HEAD
		require.NoErrorf(t, err, "concurrent request %d failed", i)
	}

	maxSeen := c.maxInflight.Load()
	assert.LessOrEqual(t, maxSeen, int32(appMaxConcurrentStreams),
		"the app's advertised stream limit must be respected, not circumvented by extra connections")
	assert.GreaterOrEqual(t, maxSeen, int32(20),
		"requests must genuinely overlap for this test to prove queueing")
=======
		assert.NoErrorf(t, err, "concurrent request %d failed", i)
	}
>>>>>>> upstream/release-1.18
}
