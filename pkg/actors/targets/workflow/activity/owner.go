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

package activity

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dapr/dapr/pkg/actors/targets/workflow/activity/inflight"
	diag "github.com/dapr/dapr/pkg/diagnostics"
	wferrors "github.com/dapr/dapr/pkg/runtime/wfengine/errors"
	"github.com/dapr/dapr/pkg/runtime/wfengine/todo"
	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend"
)

<<<<<<< HEAD
// InflightCacheTTL keeps the cached outcome of a finished activity around for
// this long so cron retries that arrive after the owner has finished (or are
// still in flight when the owner finishes) become followers and ack SUCCESS
// without dispatching a duplicate WorkItem to the SDK.
const InflightCacheTTL = 60 * time.Second

// execution is one activity execution in flight: the WorkItem handed to the
// engine plus everything needed to publish its result and finalise its
// inflight entry. Every field is an argument snapshot or factory-level
// read-only state, so the detached publish watcher remains valid after the
// *activity it started on has been recycled (HaltAll/HaltNonHosted).
type execution struct {
	actorID      string
	key          string
	call         *inflight.Call
	unregister   func()
	callback     chan bool
	wi           *backend.ActivityWorkItem
	activityName string
	workflowID   string

	// start anchors the execution-duration metric; set once the engine has
	// accepted the WorkItem.
	start time.Time
}

// settle finalises the inflight entry for key with an execution's outcome.
// Finish comes FIRST and the release second: the reverse order opens a window
// in which a new arrival becomes owner of a fresh call while followers are
// still parked on this one, and so dispatches a second work item for the same
// task. A success is cached for InflightCacheTTL so a retry that arrived while
// the owner ran acks as a follower; a failure is released at once so later
// retries contend fresh instead of reading it for the full TTL.
func (f *factory) settle(key string, call *inflight.Call, err error) {
	call.Finish(err)
	if err == nil {
		f.inflight.ReleaseAfter(key, call, InflightCacheTTL)
		return
	}
	f.inflight.Release(key, call)
}

// runOwned drives a single activity execution from WorkItem dispatch through
// to publishing the result back to the workflow actor. Exactly one arrival per
// inflight key runs it at a time (the owner), outside the actor lock: the
// inflight entry claimed in executeActivity is the execution guard. On caller
// ctx cancellation it hands off to the factory's detached watcher so the
// WorkItem already in the durabletask queue still has its result published,
// and returns ctx.Err() so the caller can surface the cancellation.
func (a *activity) runOwned(ctx context.Context, key string, call *inflight.Call, activityName, workflowID string, taskEvent *backend.HistoryEvent, invocation *protos.ActivityInvocation) error {
	// The app reports completion through this callback channel; there is no
	// execution timeout, activities may run for hours.
	callback := make(chan bool, 1)
	ex := &execution{
		actorID:      a.actorID,
		key:          key,
		call:         call,
		unregister:   func() {},
		callback:     callback,
		activityName: activityName,
		workflowID:   workflowID,
		wi: &backend.ActivityWorkItem{
			SequenceNumber:  int64(taskEvent.GetEventId()),
			InstanceID:      api.InstanceID(workflowID),
			NewEvent:        taskEvent,
			Properties:      map[string]any{todo.CallbackChannelProperty: callback},
			IncomingHistory: invocation.GetPropagatedHistory(),
		},
	}
	if a.registerResolver != nil {
		ex.unregister = a.registerResolver(workflowID, taskEvent.GetEventId(), func() { call.BeginResolve() })
	}

	log.Debugf("Activity actor '%s': scheduling activity '%s' for workflow with instanceId '%s'", a.actorID, activityReminderName, ex.wi.InstanceID)
	start := time.Now()
	err := a.scheduler(ctx, ex.wi)
	elapsed := diag.ElapsedSince(start)

	if err != nil {
		diag.DefaultWorkflowMonitoring.ActivityOperationEvent(ctx, activityName, diag.StatusRecoverable, elapsed)
		wfErr := wferrors.NewRecoverable(fmt.Errorf("failed to schedule an activity execution: %w", err))
		if errors.Is(err, context.DeadlineExceeded) {
			wfErr = wferrors.NewRecoverable(fmt.Errorf("timed-out trying to schedule an activity execution - this can happen if too many activities are running in parallel or if the workflow engine isn't running: %w", err))
		}
		ex.unregister()
		a.settle(key, call, wfErr)
=======
// inflightCacheTTL keeps the cached outcome of a finished activity around for
// this long so cron retries that arrive after the owner has finished (or are
// still in flight when the owner finishes) become followers and ack SUCCESS
// without dispatching a duplicate WorkItem to the SDK.
const inflightCacheTTL = 60 * time.Second

// runOwned drives a single activity execution from WorkItem dispatch through
// to publishing the result back to the workflow actor. It is invoked by
// exactly one reminder per activity actor at a time (the owner). On caller
// ctx cancellation it hands off to a background watcher so the WorkItem
// already in the durabletask queue still has its result published; the
// owner returns ctx.Err() so the actor framework can release its lock and
// preserve turn-based semantics.
func (a *activity) runOwned(ctx context.Context, key string, call *inflight.Call, name, activityName, workflowID string, taskEvent *backend.HistoryEvent, invocation *protos.ActivityInvocation) error {
	wi := &backend.ActivityWorkItem{
		SequenceNumber:  int64(taskEvent.GetEventId()),
		InstanceID:      api.InstanceID(workflowID),
		NewEvent:        taskEvent,
		Properties:      make(map[string]any),
		IncomingHistory: invocation.GetPropagatedHistory(),
	}

	// Executing activity code is a one-way operation. We must wait for the app code to report its completion, which
	// will trigger this callback channel.
	// TODO: Need to come up with a design for timeouts. Some activities may need to run for hours but we also need
	//       to handle the case where the app crashes and never responds to the workflow. It may be necessary to
	//       introduce some kind of heartbeat protocol to help identify such cases.
	callback := make(chan bool, 1)
	wi.Properties[todo.CallbackChannelProperty] = callback
	log.Debugf("Activity actor '%s': scheduling activity '%s' for workflow with instanceId '%s'", a.actorID, name, wi.InstanceID)
	start := time.Now()
	err := a.scheduler(ctx, wi)
	elapsed := diag.ElapsedSince(start)

	if errors.Is(err, context.DeadlineExceeded) {
		diag.DefaultWorkflowMonitoring.ActivityOperationEvent(ctx, activityName, diag.StatusRecoverable, elapsed)
		wfErr := wferrors.NewRecoverable(fmt.Errorf("timed-out trying to schedule an activity execution - this can happen if too many activities are running in parallel or if the workflow engine isn't running: %w", err))
		a.inflight.Release(key, call)
		call.Finish(wfErr)
		return wfErr
	} else if err != nil {
		diag.DefaultWorkflowMonitoring.ActivityOperationEvent(ctx, activityName, diag.StatusRecoverable, elapsed)
		wfErr := wferrors.NewRecoverable(fmt.Errorf("failed to schedule an activity execution: %w", err))
		a.inflight.Release(key, call)
		call.Finish(wfErr)
>>>>>>> upstream/release-1.18
		return wfErr
	}
	diag.DefaultWorkflowMonitoring.ActivityOperationEvent(ctx, activityName, diag.StatusSuccess, elapsed)

<<<<<<< HEAD
	// If ctx cancels before the SDK callback (the WatchJobs stream dropped,
	// or the actor framework cancelled because the app went unhealthy), hand
	// off to the detached watcher so the queued WorkItem still has its result
	// published and the inflight entry finalised; retries arriving meanwhile
	// follow the watcher's outcome via call.Done.
	ex.start = time.Now()
	select {
	case <-ctx.Done():
		a.watchAndPublish(ctx, ex)
		return ctx.Err()
	case completed := <-callback:
		if cancelBeforePublishForTest() {
			// TEST INJECTION: the result is in hand but the context carrying
			// it is cut, exactly as a placement drain leaves it.
			log.Warnf("TEST INJECTION: cancelling before publish for activity actor '%s'", a.actorID)
			var cancel context.CancelFunc
			ctx, cancel = context.WithCancel(ctx)
			cancel()
		}
		return a.publishOwned(ctx, ex, completed)
=======
	// Wait for the SDK callback OR ctx cancellation. If ctx cancels first
	// (e.g. the WatchJobs stream that delivered this reminder dropped, or
	// the actor framework is signalling cancel because the app went
	// unhealthy), hand off to a background watcher so the WorkItem already
	// in the durabletask queue still has its eventual result published and
	// the inflight entry is finalised. Returning ctx.Err() here releases the
	// actor lock so cron retries can become followers and observe the
	// watcher's outcome via call.Done.
	start = time.Now()
	select {
	case <-ctx.Done():
		// Snapshot the actorID since the *activity may be recycled
		// (HaltAll/HaltNonHosted) before the watcher finishes; everything
		// else the watcher uses is factory-level read-only state or args.
		go a.watchAndPublish(ctx, a.actorID, key, call, callback, wi, taskEvent, name, activityName, workflowID, start)
		return ctx.Err()
	case completed := <-callback:
		execErr := a.publishResult(ctx, a.actorID, completed, wi, taskEvent, name, activityName, workflowID, start)
		call.Finish(execErr)
		// On success, cache the outcome briefly so a cron retry that arrived
		// while we were running becomes a follower and acks SUCCESS without
		// dispatching a duplicate WorkItem. On error, release immediately so
		// subsequent cron retries become fresh owners and can re-attempt
		// rather than reading the cached failure for the full TTL window.
		if execErr == nil {
			a.inflight.ReleaseAfter(key, call, inflightCacheTTL)
		} else {
			a.inflight.Release(key, call)
		}
		return execErr
>>>>>>> upstream/release-1.18
	}
}
