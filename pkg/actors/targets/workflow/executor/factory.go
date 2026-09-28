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

package executor

import (
	"context"
	"sync"

	"github.com/dapr/dapr/pkg/actors"
	"github.com/dapr/dapr/pkg/actors/api"
	"github.com/dapr/dapr/pkg/actors/internal/placement"
	"github.com/dapr/dapr/pkg/actors/targets"
<<<<<<< HEAD
	"github.com/dapr/dapr/pkg/actors/targets/workflow/common/lock"
	"github.com/dapr/dapr/pkg/actors/targets/workflow/executor/pending"
	internalsv1pb "github.com/dapr/dapr/pkg/proto/internals/v1"
)

func newExecutor() *executor {
	return &executor{
		lock: lock.New(),
	}
}

=======
)

>>>>>>> upstream/release-1.18
type Options struct {
	Actors actors.Interface

	ActorType string

	// Pending is the process-local completion rendezvous shared with the
	// cluster tasks backend. Completions arriving at this actor are
	// delivered into it when the waiter is registered on this host.
	Pending *pending.Pending
}

type factory struct {
	actorType string

	actors       actors.Interface
	placement    placement.Interface
	deactivateCh chan *executor
	pending      *pending.Pending

	table sync.Map
	lock  sync.Mutex
}

func New(ctx context.Context, opts Options) (targets.Factory, error) {
	placement, err := opts.Actors.Placement(ctx)
	if err != nil {
		return nil, err
	}

	deactivateCh := make(chan *executor, 100)
	go func() {
		for executor := range deactivateCh {
			executor.deactivateIfIdle()
		}
	}()

	return &factory{
		actorType:    opts.ActorType,
		actors:       opts.Actors,
		placement:    placement,
		deactivateCh: deactivateCh,
		pending:      opts.Pending,
	}, nil
}

// GetOrCreate replaces a closed entry that has not yet left the table.
func (f *factory) GetOrCreate(actorID string) targets.Interface {
<<<<<<< HEAD
	a, ok := f.table.Load(actorID)
	if !ok {
		fresh := f.initExecutor(newExecutor(), actorID)
		a, _ = f.table.LoadOrStore(actorID, fresh)
	}
=======
	for {
		a, ok := f.table.Load(actorID)
		if !ok {
			a, _ = f.table.LoadOrStore(actorID, f.newExecutor(actorID))
		}
>>>>>>> upstream/release-1.18

		e := a.(*executor)
		if !e.isClosed() {
			return e
		}
		f.table.CompareAndDelete(actorID, e)
	}
}

<<<<<<< HEAD
func (f *factory) initExecutor(a any, actorID string) *executor {
	act := a.(*executor)

	act.factory = f
	act.actorID = actorID

	act.closed.Store(false)
	act.cancelClosed.Store(false)
	act.completeCh = make(chan *internalsv1pb.InternalInvokeResponse, 1)
	act.cancelCh = make(chan struct{})
	act.closeCh = make(chan struct{})
	act.watchLock = make(chan struct{}, 1)

	return act
=======
func (f *factory) newExecutor(actorID string) *executor {
	return &executor{
		factory:   f,
		actorID:   actorID,
		closeCh:   make(chan struct{}),
		watchLock: make(chan struct{}, 1),
	}
>>>>>>> upstream/release-1.18
}

func (f *factory) HaltAll(ctx context.Context) error {
	f.lock.Lock()
	defer f.lock.Unlock()

	f.table.Range(func(key, val any) bool {
		val.(*executor).Deactivate(ctx)
		return true
	})
	f.table.Clear()
	return nil
}

func (f *factory) HaltNonHosted(ctx context.Context, fn func(*api.LookupActorRequest) bool) error {
	f.lock.Lock()
	defer f.lock.Unlock()

	f.table.Range(func(key, val any) bool {
		if !fn(&api.LookupActorRequest{
			ActorType: f.actorType,
			ActorID:   key.(string),
		}) {
			val.(*executor).Deactivate(ctx)
		}
		return true
	})
	return nil
}

func (f *factory) Exists(actorID string) bool {
	_, ok := f.table.Load(actorID)
	return ok
}

func (f *factory) Len() int {
	var count int
	f.table.Range(func(_, _ any) bool { count++; return true })
	return count
}
