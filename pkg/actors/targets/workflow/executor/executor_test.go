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

package executor

import (
<<<<<<< HEAD
	"testing"
	"time"

=======
	"context"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
>>>>>>> upstream/release-1.18
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

<<<<<<< HEAD
	"github.com/dapr/dapr/pkg/actors/targets/workflow/executor/pending"
=======
	targeterrors "github.com/dapr/dapr/pkg/actors/targets/errors"
>>>>>>> upstream/release-1.18
	invokev1 "github.com/dapr/dapr/pkg/messaging/v1"
	internalsv1pb "github.com/dapr/dapr/pkg/proto/internals/v1"
)

<<<<<<< HEAD
// newClaimTestExecutor builds an executor without the factory goroutines so
// deactivation requests can be asserted on the buffered channel directly. The
// workflow-shaped actor ID has no sibling rendezvous form, keeping complete()
// off the forwarding path, which needs a live actor router.
func newClaimTestExecutor(t *testing.T) (*executor, *factory) {
	t.Helper()
	f := &factory{
		actorType:    "dapr.internal.default.test.executor",
		deactivateCh: make(chan *executor, 10),
		pending:      pending.New(),
	}
	e, ok := f.GetOrCreate("abc").(*executor)
	require.True(t, ok)
	return e, f
}

func claimReq(taskType string) *internalsv1pb.InternalInvokeRequest {
	return internalsv1pb.NewInternalInvokeRequest(MethodClaim).
		WithActor("dapr.internal.default.test.executor", "abc").
		WithContentType(invokev1.ProtobufContentType).
		WithMetadata(map[string][]string{MetadataTaskType: {taskType}})
}

func completeReq(taskType string, data []byte) *internalsv1pb.InternalInvokeRequest {
	return internalsv1pb.NewInternalInvokeRequest(MethodComplete).
		WithActor("dapr.internal.default.test.executor", "abc").
		WithData(data).
		WithContentType(invokev1.ProtobufContentType).
		WithMetadata(map[string][]string{MetadataTaskType: {taskType}})
}

func cancelReq(taskType string) *internalsv1pb.InternalInvokeRequest {
	return internalsv1pb.NewInternalInvokeRequest(MethodCancel).
		WithActor("dapr.internal.default.test.executor", "abc").
		WithContentType(invokev1.ProtobufContentType).
		WithMetadata(map[string][]string{MetadataTaskType: {taskType}})
}

// Test_claimCompleteRace hammers the interleaving where a waiter's whole
// Register+Claim lands between a completer's pending-map miss and its channel
// park: without the executor's rendezvous mutex the claim can observe an
// empty channel, the park lands afterwards with no reader, and the waiter
// hangs. The window is a few instructions wide, so this cannot reliably
// reproduce the unsynchronized bug; the guarantee is the mu critical
// sections in complete/claim. This keeps regression pressure on the
// invariant: every iteration must resolve the waiter via either the claim or
// the pending map, whichever side wins the race.
func Test_claimCompleteRace(t *testing.T) {
	t.Parallel()

	for range 10_000 {
		f := &factory{
			actorType:    "dapr.internal.default.test.executor",
			deactivateCh: make(chan *executor, 10),
			pending:      pending.New(),
		}
		e, ok := f.GetOrCreate("abc").(*executor)
		require.True(t, ok)

		completerDone := make(chan error, 1)
		go func() {
			_, err := e.InvokeMethod(t.Context(), completeReq(TaskTypeWorkflow, []byte("payload")))
			completerDone <- err
		}()

		waitCh := make(chan pending.Result, 1)
		deregister := f.pending.RegisterCallback(PendingKey(TaskTypeWorkflow, "abc"), func(r pending.Result) {
			select {
			case waitCh <- r:
			default:
			}
		})

		res, err := e.InvokeMethod(t.Context(), claimReq(TaskTypeWorkflow))
		require.NoError(t, err)

		switch res.GetStatus().GetCode() {
		case int32(codes.OK):
			assert.Equal(t, []byte("payload"), res.GetMessage().GetData().GetValue())
		case int32(codes.NotFound):
			select {
			case pres := <-waitCh:
				assert.Equal(t, []byte("payload"), pres.Data)
			case <-time.After(time.Second * 5):
				t.Fatal("completion neither claimed nor delivered to the pending map waiter")
			}
		default:
			t.Fatalf("unexpected claim status %d", res.GetStatus().GetCode())
		}

		deregister()
		require.NoError(t, <-completerDone)
	}
}

func Test_claim(t *testing.T) {
	t.Parallel()

	t.Run("not found when nothing parked, actor deactivates", func(t *testing.T) {
		t.Parallel()
		e, f := newClaimTestExecutor(t)

		res, err := e.InvokeMethod(t.Context(), claimReq(TaskTypeWorkflow))
		require.NoError(t, err)
		assert.Equal(t, int32(codes.NotFound), res.GetStatus().GetCode())
		assert.Len(t, f.deactivateCh, 1)
	})

	t.Run("returns completion parked before registration", func(t *testing.T) {
		t.Parallel()
		e, f := newClaimTestExecutor(t)

		_, err := e.InvokeMethod(t.Context(), completeReq(TaskTypeWorkflow, []byte("payload")))
		require.NoError(t, err)

		res, err := e.InvokeMethod(t.Context(), claimReq(TaskTypeWorkflow))
		require.NoError(t, err)
		assert.Equal(t, int32(codes.OK), res.GetStatus().GetCode())
		assert.Equal(t, []byte("payload"), res.GetMessage().GetData().GetValue())
		assert.Equal(t, TaskTypeWorkflow, res.GetHeaders()[MetadataTaskType].GetValues()[0])
		assert.Len(t, f.deactivateCh, 1)

		res, err = e.InvokeMethod(t.Context(), claimReq(TaskTypeWorkflow))
		require.NoError(t, err)
		assert.Equal(t, int32(codes.NotFound), res.GetStatus().GetCode())
	})

	t.Run("aborted when cancellation parked before registration", func(t *testing.T) {
		t.Parallel()
		e, f := newClaimTestExecutor(t)

		_, err := e.InvokeMethod(t.Context(), cancelReq(TaskTypeActivity))
		require.NoError(t, err)

		res, err := e.InvokeMethod(t.Context(), claimReq(TaskTypeActivity))
		require.NoError(t, err)
		assert.Equal(t, int32(codes.Aborted), res.GetStatus().GetCode())
		assert.Len(t, f.deactivateCh, 1)
	})

	t.Run("parked completion takes precedence over cancellation", func(t *testing.T) {
		t.Parallel()
		e, _ := newClaimTestExecutor(t)

		_, err := e.InvokeMethod(t.Context(), completeReq(TaskTypeWorkflow, []byte("payload")))
		require.NoError(t, err)
		_, err = e.InvokeMethod(t.Context(), cancelReq(TaskTypeWorkflow))
		require.NoError(t, err)

		res, err := e.InvokeMethod(t.Context(), claimReq(TaskTypeWorkflow))
		require.NoError(t, err)
		assert.Equal(t, int32(codes.OK), res.GetStatus().GetCode())
		assert.Equal(t, []byte("payload"), res.GetMessage().GetData().GetValue())
	})

	t.Run("other task type's completion is put back, not claimed", func(t *testing.T) {
		t.Parallel()
		e, f := newClaimTestExecutor(t)

		_, err := e.InvokeMethod(t.Context(), completeReq(TaskTypeWorkflow, []byte("payload")))
		require.NoError(t, err)

		res, err := e.InvokeMethod(t.Context(), claimReq(TaskTypeActivity))
		require.NoError(t, err)
		assert.Equal(t, int32(codes.NotFound), res.GetStatus().GetCode())
		assert.Empty(t, f.deactivateCh, "put-back must keep the actor alive for its own waiter")

		res, err = e.InvokeMethod(t.Context(), claimReq(TaskTypeWorkflow))
		require.NoError(t, err)
		assert.Equal(t, int32(codes.OK), res.GetStatus().GetCode())
		assert.Equal(t, []byte("payload"), res.GetMessage().GetData().GetValue())
	})

	t.Run("displaced completion is claimed before the channel", func(t *testing.T) {
		t.Parallel()
		e, _ := newClaimTestExecutor(t)

		_, err := e.InvokeMethod(t.Context(), completeReq(TaskTypeWorkflow, []byte("first")))
		require.NoError(t, err)

		// The wrong-type claim moves "first" to the displaced slot, freeing
		// the channel for "second".
		res, err := e.InvokeMethod(t.Context(), claimReq(TaskTypeActivity))
		require.NoError(t, err)
		assert.Equal(t, int32(codes.NotFound), res.GetStatus().GetCode())

		_, err = e.InvokeMethod(t.Context(), completeReq(TaskTypeWorkflow, []byte("second")))
		require.NoError(t, err)

		for _, want := range [][]byte{[]byte("first"), []byte("second")} {
			res, err = e.InvokeMethod(t.Context(), claimReq(TaskTypeWorkflow))
			require.NoError(t, err)
			assert.Equal(t, int32(codes.OK), res.GetStatus().GetCode())
			assert.Equal(t, want, res.GetMessage().GetData().GetValue())
		}
	})

	t.Run("no completion is lost to a concurrent blocked completer", func(t *testing.T) {
		t.Parallel()
		e, f := newClaimTestExecutor(t)

		// Fill the channel slot with a workflow-type completion, then block an
		// activity-type completer on the full channel: the moment a claim
		// drains the slot this sender refills it, which is exactly the window
		// where a channel put-back would drop the drained payload. The two
		// payloads are of different task types, so neither may ever be lost.
		_, err := e.InvokeMethod(t.Context(), completeReq(TaskTypeWorkflow, []byte("wf")))
		require.NoError(t, err)
		blockedDone := make(chan error, 1)
		go func() {
			_, berr := e.InvokeMethod(t.Context(), completeReq(TaskTypeActivity, []byte("act")))
			blockedDone <- berr
		}()

		// The activity claim displaces "wf"; depending on whether the blocked
		// sender has refilled the slot yet it returns "act" or not-found.
		res, err := e.InvokeMethod(t.Context(), claimReq(TaskTypeActivity))
		require.NoError(t, err)
		gotAct := res.GetStatus().GetCode() == int32(codes.OK)
		if gotAct {
			assert.Equal(t, []byte("act"), res.GetMessage().GetData().GetValue())
		}
		assert.Empty(t, f.deactivateCh, "a displaced completion must keep the actor alive")

		res, err = e.InvokeMethod(t.Context(), claimReq(TaskTypeWorkflow))
		require.NoError(t, err)
		assert.Equal(t, int32(codes.OK), res.GetStatus().GetCode())
		assert.Equal(t, []byte("wf"), res.GetMessage().GetData().GetValue())

		if !gotAct {
			assert.EventuallyWithT(t, func(col *assert.CollectT) {
				cres, cerr := e.InvokeMethod(t.Context(), claimReq(TaskTypeActivity))
				if !assert.NoError(col, cerr) {
					return
				}
				if assert.Equal(col, int32(codes.OK), cres.GetStatus().GetCode()) {
					assert.Equal(col, []byte("act"), cres.GetMessage().GetData().GetValue())
				}
			}, time.Second*5, time.Millisecond)
		}
		require.NoError(t, <-blockedDone)
	})

	t.Run("delivers to registered waiter instead of parking", func(t *testing.T) {
		t.Parallel()
		e, f := newClaimTestExecutor(t)

		waitCh := make(chan pending.Result, 1)
		deregister := f.pending.RegisterCallback(PendingKey(TaskTypeWorkflow, "abc"), func(r pending.Result) {
			select {
			case waitCh <- r:
			default:
			}
		})
		defer deregister()

		_, err := e.InvokeMethod(t.Context(), completeReq(TaskTypeWorkflow, []byte("payload")))
		require.NoError(t, err)

		select {
		case res := <-waitCh:
			assert.Equal(t, []byte("payload"), res.Data)
		default:
			t.Fatal("completion was not delivered to the registered waiter")
		}

		// A successful delivery also parks a copy for stale watch streams;
		// a subsequent claim drains that copy (duplicates are discarded by
		// the workflow-side dedup guards).
		res, err := e.InvokeMethod(t.Context(), claimReq(TaskTypeWorkflow))
		require.NoError(t, err)
		assert.Equal(t, int32(codes.OK), res.GetStatus().GetCode())
		assert.Equal(t, []byte("payload"), res.GetMessage().GetData().GetValue())
	})
=======
const (
	testActorType = "dapr.internal.default.test.executor"
	testActorID   = "abc"
)

// newTestFactory builds a factory without placement and drains its
// deactivation queue like New does.
func newTestFactory(t *testing.T) *factory {
	t.Helper()
	f := &factory{
		actorType:    testActorType,
		deactivateCh: make(chan *executor, 100),
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for e := range f.deactivateCh {
			e.deactivateIfIdle()
		}
	}()
	t.Cleanup(func() {
		close(f.deactivateCh)
		<-done
	})
	return f
}

func methodReq(method string) *internalsv1pb.InternalInvokeRequest {
	return internalsv1pb.NewInternalInvokeRequest(method).
		WithActor(testActorType, testActorID).
		WithContentType(invokev1.ProtobufContentType)
}

func completeReq(data []byte) *internalsv1pb.InternalInvokeRequest {
	return methodReq(MethodComplete).WithData(data)
}

type watchResult struct {
	res *internalsv1pb.InternalInvokeResponse
	err error
}

// watch attaches a WatchComplete stream and reports its single response or
// error.
func watch(ctx context.Context, e *executor) <-chan watchResult {
	out := make(chan watchResult, 1)
	go func() {
		var got *internalsv1pb.InternalInvokeResponse
		err := e.InvokeStream(ctx, methodReq(MethodWatchComplete), func(r *internalsv1pb.InternalInvokeResponse) (bool, error) {
			got = r
			return true, nil
		})
		out <- watchResult{res: got, err: err}
	}()
	return out
}

func waitAttached(t *testing.T, e *executor) {
	t.Helper()
	require.Eventually(t, func() bool {
		e.mu.Lock()
		defer e.mu.Unlock()
		return e.watcher != nil
	}, 5*time.Second, time.Millisecond)
}

func register(t *testing.T, e *executor) {
	t.Helper()
	_, err := e.InvokeMethod(t.Context(), methodReq(MethodRegister))
	require.NoError(t, err)
}

func complete(t *testing.T, e *executor, data string) {
	t.Helper()
	_, err := e.InvokeMethod(t.Context(), completeReq([]byte(data)))
	require.NoError(t, err)
}

func payload(res *internalsv1pb.InternalInvokeResponse) string {
	return string(res.GetMessage().GetData().GetValue())
}

func Test_unregisteredCompletionNeverAnswersRegisteredWaiter(t *testing.T) {
	t.Parallel()

	f := newTestFactory(t)
	e := f.GetOrCreate(testActorID).(*executor)

	complete(t, e, "orphan")

	e.mu.Lock()
	assert.NotNil(t, e.parked)
	e.mu.Unlock()

	register(t, e)
	e.mu.Lock()
	assert.Nil(t, e.parked, "a registration discards the unregistered completion")
	e.mu.Unlock()

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	res := <-watch(ctx, e)
	require.ErrorIs(t, res.err, context.DeadlineExceeded)
	assert.Nil(t, res.res, "the orphan completion must not answer the next waiter")
}

func Test_unregisteredWatcherTakesUnregisteredCompletion(t *testing.T) {
	t.Parallel()

	f := newTestFactory(t)
	e := f.GetOrCreate(testActorID).(*executor)

	// A pre-Register daprd: completion first, then a bare watch.
	complete(t, e, "legacy")
	e.deactivateIfIdle()
	assert.False(t, e.isClosed(), "a parked completion keeps the actor alive")

	res := <-watch(t.Context(), e)
	require.NoError(t, res.err)
	assert.Equal(t, "legacy", payload(res.res))

	require.Eventually(t, e.isClosed, 5*time.Second, time.Millisecond)
}

func Test_registerParkedThenWatch(t *testing.T) {
	t.Parallel()

	f := newTestFactory(t)
	e := f.GetOrCreate(testActorID).(*executor)

	register(t, e)
	complete(t, e, "one")

	res := <-watch(t.Context(), e)
	require.NoError(t, res.err)
	assert.Equal(t, "one", payload(res.res))
}

func Test_registerWatchThenComplete(t *testing.T) {
	t.Parallel()

	f := newTestFactory(t)
	e := f.GetOrCreate(testActorID).(*executor)

	register(t, e)
	out := watch(t.Context(), e)
	waitAttached(t, e)
	complete(t, e, "live")

	res := <-out
	require.NoError(t, res.err)
	assert.Equal(t, "live", payload(res.res))
}

func Test_watchWithoutRegisterStillReceives(t *testing.T) {
	t.Parallel()

	f := newTestFactory(t)
	e := f.GetOrCreate(testActorID).(*executor)

	out := watch(t.Context(), e)
	waitAttached(t, e)
	complete(t, e, "legacy")

	res := <-out
	require.NoError(t, res.err)
	assert.Equal(t, "legacy", payload(res.res))
}

func Test_duplicateCompletionIsDropped(t *testing.T) {
	t.Parallel()

	f := newTestFactory(t)
	e := f.GetOrCreate(testActorID).(*executor)

	register(t, e)
	complete(t, e, "first")
	complete(t, e, "second")

	res := <-watch(t.Context(), e)
	require.NoError(t, res.err)
	assert.Equal(t, "first", payload(res.res))

	// Nothing is left for a later waiter.
	e = next(t, f, e)
	register(t, e)
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	res = <-watch(ctx, e)
	require.ErrorIs(t, res.err, context.DeadlineExceeded)
}

// e2 returns the live executor for the test actor ID.
func e2(t *testing.T, f *factory) *executor {
	t.Helper()
	return f.GetOrCreate(testActorID).(*executor)
}

// next waits for prev's idle deactivation and returns its successor, so the
// test does not race the deactivation queue the way the router's closed-actor
// retry absorbs in production.
func next(t *testing.T, f *factory, prev *executor) *executor {
	t.Helper()
	require.Eventually(t, prev.isClosed, 5*time.Second, time.Millisecond)
	return e2(t, f)
}

func Test_watcherDepartureDiscardsLateCompletion(t *testing.T) {
	t.Parallel()

	f := newTestFactory(t)
	e := f.GetOrCreate(testActorID).(*executor)

	register(t, e)
	ctx, cancel := context.WithCancel(t.Context())
	out := watch(ctx, e)
	waitAttached(t, e)
	cancel()
	res := <-out
	require.ErrorIs(t, res.err, context.Canceled)

	// The aborted execution's late response lands on the successor, where it
	// is parked with no registration.
	live := next(t, f, e)
	complete(t, live, "late")

	// The retried turn's registration discards it and gets only its own.
	register(t, live)
	live.mu.Lock()
	assert.Nil(t, live.parked)
	live.mu.Unlock()
	complete(t, live, "fresh")
	res = <-watch(t.Context(), live)
	require.NoError(t, res.err)
	assert.Equal(t, "fresh", payload(res.res))
}

func Test_registerDiscardsStaleParked(t *testing.T) {
	t.Parallel()

	f := newTestFactory(t)
	e := f.GetOrCreate(testActorID).(*executor)

	// A registration whose watcher never came leaves a stale parked payload.
	register(t, e)
	complete(t, e, "stale")

	register(t, e)
	e.mu.Lock()
	assert.Nil(t, e.parked)
	e.mu.Unlock()

	complete(t, e, "fresh")
	res := <-watch(t.Context(), e)
	require.NoError(t, res.err)
	assert.Equal(t, "fresh", payload(res.res))
}

func Test_idleDeactivation(t *testing.T) {
	t.Parallel()

	f := newTestFactory(t)
	e := f.GetOrCreate(testActorID).(*executor)

	// Armed: an idle deactivation request is a no-op.
	register(t, e)
	e.deactivateIfIdle()
	assert.False(t, e.isClosed())
	assert.Same(t, e, f.GetOrCreate(testActorID))

	// Attached: still a no-op.
	out := watch(t.Context(), e)
	waitAttached(t, e)
	e.deactivateIfIdle()
	assert.False(t, e.isClosed())

	complete(t, e, "one")
	res := <-out
	require.NoError(t, res.err)

	// The watcher's exit deactivates the idle actor.
	require.Eventually(t, e.isClosed, 5*time.Second, time.Millisecond)
	require.Eventually(t, func() bool { return !f.Exists(testActorID) }, 5*time.Second, time.Millisecond)

	_, err := e.InvokeMethod(t.Context(), methodReq(MethodRegister))
	require.True(t, targeterrors.IsClosed(err), "a call landing on the closed actor must be retriable: %v", err)
	_, err = e.InvokeMethod(t.Context(), completeReq([]byte("x")))
	require.True(t, targeterrors.IsClosed(err))
	err = e.InvokeStream(t.Context(), methodReq(MethodWatchComplete), func(*internalsv1pb.InternalInvokeResponse) (bool, error) { return true, nil })
	require.True(t, targeterrors.IsClosed(err))

	fresh := f.GetOrCreate(testActorID).(*executor)
	assert.NotSame(t, e, fresh)
	assert.False(t, fresh.isClosed())
}

func Test_getOrCreateSkipsClosedEntry(t *testing.T) {
	t.Parallel()

	f := newTestFactory(t)
	e := f.GetOrCreate(testActorID).(*executor)

	// Closed but still in the table, as a load racing the close observes.
	e.mu.Lock()
	e.closed = true
	close(e.closeCh)
	e.mu.Unlock()

	fresh := f.GetOrCreate(testActorID).(*executor)
	assert.NotSame(t, e, fresh)
	assert.False(t, fresh.isClosed())
	assert.Same(t, fresh, f.GetOrCreate(testActorID))
}

func Test_forcedDeactivateAbortsAttachedWatcher(t *testing.T) {
	t.Parallel()

	f := newTestFactory(t)
	e := f.GetOrCreate(testActorID).(*executor)

	register(t, e)
	out := watch(t.Context(), e)
	waitAttached(t, e)

	require.NoError(t, e.Deactivate(t.Context()))
	res := <-out
	var perm *backoff.PermanentError
	require.ErrorAs(t, res.err, &perm, "a halt must abort the turn, not retry it")
	assert.False(t, f.Exists(testActorID))
}

func Test_cancel(t *testing.T) {
	t.Parallel()

	f := newTestFactory(t)
	e := f.GetOrCreate(testActorID).(*executor)

	// Cancel is idempotent and a no-op with nothing armed.
	_, err := e.InvokeMethod(t.Context(), methodReq(MethodCancel))
	require.NoError(t, err)
	_, err = e.InvokeMethod(t.Context(), methodReq(MethodCancel))
	require.NoError(t, err)

	// Cancel before the watcher attaches.
	register(t, e)
	_, err = e.InvokeMethod(t.Context(), methodReq(MethodCancel))
	require.NoError(t, err)
	res := <-watch(t.Context(), e)
	require.NoError(t, res.err)
	assert.Equal(t, int32(codes.Aborted), res.res.GetStatus().GetCode())

	// Cancel with the watcher attached.
	e = next(t, f, e)
	register(t, e)
	out := watch(t.Context(), e)
	waitAttached(t, e)
	_, err = e.InvokeMethod(t.Context(), methodReq(MethodCancel))
	require.NoError(t, err)
	res = <-out
	require.NoError(t, res.err)
	assert.Equal(t, int32(codes.Aborted), res.res.GetStatus().GetCode())

	// A cancel with nothing registered aborts the next unregistered watcher,
	// as before, and is reset by a registration.
	e = next(t, f, e)
	_, err = e.InvokeMethod(t.Context(), methodReq(MethodCancel))
	require.NoError(t, err)
	res = <-watch(t.Context(), e)
	require.NoError(t, res.err)
	assert.Equal(t, int32(codes.Aborted), res.res.GetStatus().GetCode())

	e = next(t, f, e)
	_, err = e.InvokeMethod(t.Context(), methodReq(MethodCancel))
	require.NoError(t, err)
	register(t, e)
	complete(t, e, "after-cancel")
	res = <-watch(t.Context(), e)
	require.NoError(t, res.err)
	assert.Equal(t, "after-cancel", payload(res.res))
}

func Test_registerWithAttachedWatcherKeepsSerialization(t *testing.T) {
	t.Parallel()

	f := newTestFactory(t)
	e := f.GetOrCreate(testActorID).(*executor)

	register(t, e)
	first := watch(t.Context(), e)
	waitAttached(t, e)

	// A registration for the same key while a watcher is attached (an
	// activity of a previous generation still in flight) neither aborts it
	// nor lets a later watcher jump the queue.
	register(t, e)
	second := watch(t.Context(), e)

	complete(t, e, "one")
	res := <-first
	require.NoError(t, res.err)
	assert.Equal(t, "one", payload(res.res))

	waitAttached(t, e)
	complete(t, e, "two")
	res = <-second
	require.NoError(t, res.err)
	assert.Equal(t, "two", payload(res.res))
}

// Test_tightLoopTurns models the ContinueAsNew tight loop: back-to-back
// exchanges on one key, each racing the previous turn's idle deactivation.
func Test_tightLoopTurns(t *testing.T) {
	t.Parallel()

	f := newTestFactory(t)

	for i := range 5_000 {
		want := []byte{byte(i), byte(i >> 8)}

		var e *executor
		for {
			e = f.GetOrCreate(testActorID).(*executor)
			_, err := e.InvokeMethod(t.Context(), methodReq(MethodRegister))
			if err == nil {
				break
			}
			require.True(t, targeterrors.IsClosed(err), "iteration %d: %v", i, err)
		}

		// The completion races the watcher attaching, as in production.
		go func() {
			for {
				_, err := e.InvokeMethod(t.Context(), completeReq(want))
				if err == nil || !targeterrors.IsClosed(err) {
					return
				}
				e = f.GetOrCreate(testActorID).(*executor)
			}
		}()

		select {
		case res := <-watch(t.Context(), e):
			require.NoError(t, res.err, "iteration %d", i)
			require.Equal(t, want, res.res.GetMessage().GetData().GetValue(), "iteration %d received another turn's completion", i)
		case <-time.After(10 * time.Second):
			t.Fatalf("iteration %d: completion neither parked nor delivered", i)
		}
	}
}

func Test_unknownMethod(t *testing.T) {
	t.Parallel()

	f := newTestFactory(t)
	e := f.GetOrCreate(testActorID).(*executor)

	_, err := e.InvokeMethod(t.Context(), methodReq("Nope"))
	require.Error(t, err)
	assert.False(t, targeterrors.IsClosed(err))
	assert.NotErrorIs(t, err, context.Canceled)
>>>>>>> upstream/release-1.18
}
