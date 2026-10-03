/*
Copyright 2026 The Dapr Authors
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

package stream

import (
	"context"
	"errors"
	"testing"
	"time"

<<<<<<< HEAD
	"github.com/dapr/dapr/pkg/actors/internal/placement/loops"
	"github.com/dapr/kit/events/loop/fake"
)

// stuckTransport simulates a peer that never answers: Recv only returns once
// its context is cancelled, never on its own.
type stuckTransport struct {
	ctx context.Context
}

func (s *stuckTransport) Recv() (*loops.Order, error) {
=======
	"google.golang.org/grpc"

	"github.com/dapr/dapr/pkg/actors/internal/placement/loops"
	v1pb "github.com/dapr/dapr/pkg/proto/placement/v1"
	"github.com/dapr/kit/events/loop/fake"
)

// stuckStream simulates a peer that never answers: Recv only returns once
// its context is cancelled, never on its own.
type stuckStream struct {
	grpc.ClientStream
	ctx context.Context
}

func (s *stuckStream) Recv() (*v1pb.PlacementOrder, error) {
>>>>>>> upstream/release-1.18
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

<<<<<<< HEAD
func (s *stuckTransport) SendReport(*loops.Report) error { return nil }
func (s *stuckTransport) SendAck(*loops.Ack) error       { return nil }
func (s *stuckTransport) CloseSend() error               { return nil }

func TestHandleShutdownAbortsStuckRecv(t *testing.T) {
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	l := New(context.Background(), Options{
		Channel:       &stuckTransport{ctx: streamCtx},
=======
func (s *stuckStream) Send(*v1pb.Host) error { return nil }
func (s *stuckStream) CloseSend() error      { return nil }

func TestHandleShutdownAbortsStuckRecv(t *testing.T) {
	streamCtx, streamCancel := context.WithCancel(t.Context())
	defer streamCancel()

	l := New(t.Context(), Options{
		Channel:       &stuckStream{ctx: streamCtx},
>>>>>>> upstream/release-1.18
		Cancel:        streamCancel,
		PlacementLoop: fake.New[loops.EventPlace](),
		IDx:           1,
	})

	runDone := make(chan error, 1)
	go func() {
<<<<<<< HEAD
		runDone <- l.Run(context.Background())
=======
		runDone <- l.Run(t.Context())
>>>>>>> upstream/release-1.18
	}()

	// Let recvLoop actually call Recv() and start blocking on it.
	time.Sleep(50 * time.Millisecond)

	closeDone := make(chan struct{})
	go func() {
		l.Close(&loops.Shutdown{Error: errors.New("test shutdown")})
		close(closeDone)
	}()

	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("Close did not return: stuck Recv() was not force-aborted")
	}

	select {
	case <-runDone:
	case <-time.After(time.Second):
		t.Fatal("stream loop Run did not return after Close")
	}
}
