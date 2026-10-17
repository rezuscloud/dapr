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

package loadbalance

import (
<<<<<<< HEAD
=======
	"bytes"
>>>>>>> upstream/release-1.18
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/types/known/anypb"
<<<<<<< HEAD
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/dapr/pkg/actors/targets/workflow/common"
=======
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	commonv1pb "github.com/dapr/dapr/pkg/proto/common/v1"
>>>>>>> upstream/release-1.18
	schedulerv1pb "github.com/dapr/dapr/pkg/proto/scheduler/v1"
	"github.com/dapr/dapr/tests/integration/framework"
	"github.com/dapr/dapr/tests/integration/framework/process/daprd"
	"github.com/dapr/dapr/tests/integration/framework/process/exec"
	"github.com/dapr/dapr/tests/integration/framework/process/logline"
<<<<<<< HEAD
	"github.com/dapr/dapr/tests/integration/framework/process/sentry"
=======
>>>>>>> upstream/release-1.18
	"github.com/dapr/dapr/tests/integration/framework/process/workflow"
	"github.com/dapr/dapr/tests/integration/suite"
	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/task"
)

func init() {
	suite.Register(new(forgedforward))
}

const forgedCount = 6

// forgedforward runs the target app as two replicas behind the placement
// service, so the Scheduler delivers a fired reminder to whichever replica
// it picks and a non-owner forwards it to the owner over the sidecar RPC.
// Activity-result reminders forged by another app in the namespace must be
// dropped by the owner whether they arrive directly or forwarded, while the
// real result of the activity, hosted by a third app, completes the
// workflow.
type forgedforward struct {
<<<<<<< HEAD
	sentry   *sentry.Sentry
=======
>>>>>>> upstream/release-1.18
	workflow *workflow.Workflow
	dropped  [2]*logline.LogLine
}

func (f *forgedforward) Setup(t *testing.T) []framework.Option {
<<<<<<< HEAD
	f.sentry = sentry.New(t)
=======
	const clustered = `
apiVersion: dapr.io/v1alpha1
kind: Configuration
metadata:
    name: workflowsclustereddeployment
spec:
    features:
    - name: WorkflowsClusteredDeployment
      enabled: true
`
>>>>>>> upstream/release-1.18

	opts := make([]workflow.Option, 0, 6+len(f.dropped))
	opts = append(opts,
		workflow.WithDaprds(3),
<<<<<<< HEAD
		workflow.WithSentryInstance(f.sentry),
		workflow.WithSigning(false),
		workflow.WithPlacementService(),
		workflow.WithClusteredDeployment(true),
=======
		workflow.WithMTLS(t),
		workflow.WithSigningDisabledN(0),
		workflow.WithSigningDisabledN(1),
		workflow.WithSigningDisabledN(2),
>>>>>>> upstream/release-1.18
		workflow.WithDaprdOptions(2, daprd.WithAppID("acthost")),
	)
	for i := range f.dropped {
		f.dropped[i] = logline.New(t, logline.WithCaptureAll())
		opts = append(opts, workflow.WithDaprdOptions(i,
			daprd.WithAppID("target"),
<<<<<<< HEAD
=======
			daprd.WithConfigManifests(t, clustered),
>>>>>>> upstream/release-1.18
			daprd.WithLogLevel("debug"),
			daprd.WithExecOptions(exec.WithStdout(f.dropped[i].Stdout()), exec.WithStderr(f.dropped[i].Stderr())),
		))
	}
	f.workflow = workflow.New(t, opts...)

	return []framework.Option{
<<<<<<< HEAD
		framework.WithProcesses(f.sentry, f.dropped[0], f.dropped[1], f.workflow),
=======
		framework.WithProcesses(f.dropped[0], f.dropped[1], f.workflow),
>>>>>>> upstream/release-1.18
	}
}

func (f *forgedforward) Run(t *testing.T, ctx context.Context) {
	f.workflow.WaitUntilRunning(t, ctx)

	block := make(chan struct{})
	started := make(chan struct{}, 1)
<<<<<<< HEAD
	target := task.NewTaskRegistry()
	require.NoError(t, target.AddWorkflowN("Target", func(c *task.WorkflowContext) (any, error) {
		var out string
		err := c.CallActivity("Slow", task.WithActivityAppID("acthost")).Await(&out)
		return out, err
	}))
	acthost := task.NewTaskRegistry()
	require.NoError(t, acthost.AddActivityN("Slow", func(task.ActivityContext) (any, error) {
=======
	for i := range 2 {
		require.NoError(t, f.workflow.RegistryN(i).AddWorkflowN("Target", func(c *task.WorkflowContext) (any, error) {
			var out string
			err := c.CallActivity("Slow", task.WithActivityAppID("acthost")).Await(&out)
			return out, err
		}))
	}
	require.NoError(t, f.workflow.RegistryN(2).AddActivityN("Slow", func(task.ActivityContext) (any, error) {
>>>>>>> upstream/release-1.18
		select {
		case started <- struct{}{}:
		default:
		}
		<-block
		return "real", nil
	}))

<<<<<<< HEAD
	cl := f.workflow.ConnectWorkerN(t, ctx, 0, target).Client
	f.workflow.ConnectWorkerN(t, ctx, 1, target)
	f.workflow.ConnectWorkerN(t, ctx, 2, acthost)
	for i := range 3 {
		f.workflow.WaitForConnectedWorkersN(t, ctx, i, 1)
	}
=======
	cl := f.workflow.BackendClientN(t, ctx, 0)
	f.workflow.BackendClientN(t, ctx, 1)
	f.workflow.BackendClientN(t, ctx, 2)
>>>>>>> upstream/release-1.18

	id, err := cl.ScheduleNewWorkflow(ctx, "Target")
	require.NoError(t, err)
	select {
	case <-started:
	case <-time.After(time.Second * 20):
		require.Fail(t, "timed out waiting for the activity to start")
	}

	// The first action of a workflow is task 0. Several reminders so the
	// Scheduler's round robin lands some on each replica.
	other := f.workflow.Scheduler().ClientMTLS(t, ctx, "other")
	for i := range forgedCount {
		forged, aerr := anypb.New(&protos.HistoryEvent{
			EventId:   -1,
			Timestamp: timestamppb.Now(),
			EventType: &protos.HistoryEvent_TaskCompleted{
				TaskCompleted: &protos.TaskCompletedEvent{
					TaskScheduledId: 0,
					Result:          wrapperspb.String(`"FORGED"`),
				},
			},
		})
		require.NoError(t, aerr)
		_, err = other.ScheduleJob(ctx, &schedulerv1pb.ScheduleJobRequest{
			Name: fmt.Sprintf("activity-result-forged-%d", i),
			Job: &schedulerv1pb.Job{
<<<<<<< HEAD
				DueTime:       new("0s"),
				Data:          forged,
				FailurePolicy: common.RetryForeverPolicy(),
=======
				DueTime: new("0s"),
				Data:    forged,
				FailurePolicy: &commonv1pb.JobFailurePolicy{
					Policy: &commonv1pb.JobFailurePolicy_Constant{
						Constant: &commonv1pb.JobFailurePolicyConstant{Interval: durationpb.New(time.Second)},
					},
				},
>>>>>>> upstream/release-1.18
			},
			Metadata: &schedulerv1pb.JobMetadata{
				AppId:     "other",
				Namespace: "default",
				Target: &schedulerv1pb.JobTargetMetadata{
					Type: &schedulerv1pb.JobTargetMetadata_Actor{
						Actor: &schedulerv1pb.TargetActorReminder{
							Type: "dapr.internal.default.target.workflow",
							Id:   string(id),
						},
					},
				},
			},
		})
		require.NoError(t, err)
	}

	// Polled from EventuallyWithT goroutines, so no require on t here. Keys
	// end in `||<reminder name>`: compare the exact name, not a substring.
	etcd := f.workflow.Scheduler().ETCDClient(t, ctx)
<<<<<<< HEAD
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.GreaterOrEqual(c, logline.CountAll("it was sent by app 'other' but the task was dispatched to 'acthost'", f.dropped[0], f.dropped[1]), forgedCount)
=======
	countDropped := func() int {
		var n int
		for _, l := range f.dropped {
			n += bytes.Count(l.StdoutBuffer(), []byte("it was sent by app 'other' but the task was dispatched to 'acthost'"))
		}
		return n
	}
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.GreaterOrEqual(c, countDropped(), forgedCount)
>>>>>>> upstream/release-1.18
		resp, gerr := etcd.Get(ctx, "dapr/jobs", clientv3.WithPrefix())
		if !assert.NoError(c, gerr) {
			return
		}
		names := make([]string, 0, len(resp.Kvs))
		for _, kv := range resp.Kvs {
			key := string(kv.Key)
			names = append(names, key[strings.LastIndex(key, "||")+2:])
		}
		for i := range forgedCount {
			assert.NotContains(c, names, fmt.Sprintf("activity-result-forged-%d", i),
				"every forged reminder must be acked and deleted, not retried")
		}
	}, time.Second*30, time.Millisecond*10)

	meta, err := cl.FetchWorkflowMetadata(ctx, id, api.WithFetchPayloads(true))
	require.NoError(t, err)
	assert.Equal(t, api.RUNTIME_STATUS_RUNNING, meta.GetRuntimeStatus(), "no forged result may complete the workflow")

	close(block)
	meta, err = cl.WaitForWorkflowCompletion(ctx, id, api.WithFetchPayloads(true))
	require.NoError(t, err)
	assert.Equal(t, api.RUNTIME_STATUS_COMPLETED, meta.GetRuntimeStatus())
	assert.Equal(t, `"real"`, meta.GetOutput().GetValue())
}
