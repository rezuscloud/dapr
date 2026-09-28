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

package processor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	compapi "github.com/dapr/dapr/pkg/apis/components/v1alpha1"
	operatorv1 "github.com/dapr/dapr/pkg/proto/operator/v1"
<<<<<<< HEAD
	"github.com/dapr/dapr/pkg/runtime/processor/loops"
	"github.com/dapr/dapr/pkg/runtime/processor/loops/root"
=======
	rterrors "github.com/dapr/dapr/pkg/runtime/errors"
	"github.com/dapr/dapr/pkg/runtime/hotreload/differ"
>>>>>>> upstream/release-1.18
)

// AddPendingComponent enqueues a component init and returns a buffered chan
// that receives exactly one error (nil on success). Returns nil if the
// processor is shut down.
func (p *Processor) AddPendingComponent(ctx context.Context, comp compapi.Component) <-chan error {
	if p.closed.Load() || ctx.Err() != nil {
		return nil
	}
	res := make(chan error, 1)
	p.rootLoop.Loop().Enqueue(&loops.Init{Component: comp, Result: res})
	return res
}

// Init synchronously initialises a component. If Process is running, the
// init is routed through the loop hierarchy and waits for the result;
// otherwise the init runs inline. Tests that drive the processor without
// calling Process rely on the inline fallback.
func (p *Processor) Init(ctx context.Context, comp compapi.Component) error {
	if !p.running.Load() {
		return p.initInline(ctx, comp)
	}
	res := p.AddPendingComponent(ctx, comp)
	if res == nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errors.New("processor is shut down")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-res:
		return err
	}
}

<<<<<<< HEAD
// Close synchronously closes a component. If Process is running, the close is
// routed through the loop; otherwise it runs inline. When routed through the
// loop the wait honours ctx so a caller is not blocked indefinitely if the
// loop is being torn down.
func (p *Processor) Close(ctx context.Context, comp compapi.Component) error {
	if !p.running.Load() {
		return p.closeInline(comp)
	}
	if p.closed.Load() {
		return p.closeInline(comp)
	}
	res := make(chan error, 1)
	p.rootLoop.Loop().Enqueue(&loops.Close{Component: comp, Result: res})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-res:
		return err
	}
}

// initInline is the synchronous, non-loop path used by tests that drive the
// processor without calling Process. It mirrors the legacy
// processComponentAndDependents + init dance.
func (p *Processor) initInline(ctx context.Context, comp compapi.Component) error {
	_, unready := p.secret.ProcessResource(ctx, &comp)
	if unready != "" {
		return nil
	}
	cat := p.category(comp)
	if cat == "" {
		return fmt.Errorf("incorrect type %s", comp.Spec.Type)
	}
	mgr, ok := p.inlineManagers[cat]
	if !ok {
		return fmt.Errorf("unknown component category: %q", cat)
	}
	timeout, err := time.ParseDuration(comp.Spec.InitTimeout)
	if err != nil || timeout <= 0 {
		timeout = root.DefaultComponentInitTimeout
	}
	initCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	initErr := p.runInlineInit(initCtx, comp, mgr)
	if errors.Is(initCtx.Err(), context.DeadlineExceeded) && initErr == nil {
		initErr = fmt.Errorf("init timeout for component %s", comp.LogName())
	}
	p.reportInline(ctx, comp, operatorv1.EventType_EVENT_INIT, initErr)
	// Match legacy proc.Init: return the sub-processor's wrapped error
	// directly. The "outer" rterrors.NewInit wrap is only applied on the
	// loop path (AddPendingComponent), not on the synchronous Init path.
	return initErr
}

func (p *Processor) runInlineInit(ctx context.Context, comp compapi.Component, mgr inlineManager) error {
=======
	// A component identical to one already installed is a no-op. This makes
	// duplicate init events idempotent: a component parked behind an unready
	// secret store is re-created by the hot reload reconciler on subsequent
	// reconciles (it is not in the component store while parked), so when the
	// secret store arrives the flushed parked copy and the reconciler's copy
	// race to init the same component.
	if existing, ok := p.compStore.GetComponent(comp.Name); ok && differ.AreSame(existing, comp) {
		log.Debugf("Component init skipped: identical component already installed: %s", comp.LogName())
		return nil
	}

>>>>>>> upstream/release-1.18
	if err := p.compStore.AddPendingComponentForCommit(comp); err != nil {
		return err
	}
	if err := mgr.Init(p.security.WithSVIDContext(ctx), comp); err != nil {
		if derr := p.compStore.DropPendingComponent(); derr != nil {
			return errors.Join(err, derr)
		}
		return err
	}
	if err := p.compStore.CommitPendingComponent(); err != nil {
		return fmt.Errorf("error committing component: %w", err)
	}
	return nil
}

func (p *Processor) closeInline(comp compapi.Component) error {
	cat := p.category(comp)
	if cat == "" {
		return fmt.Errorf("incorrect type %s", comp.Spec.Type)
	}
	mgr, ok := p.inlineManagers[cat]
	if !ok {
		return fmt.Errorf("unknown component category: %q", cat)
	}
	closeErr := mgr.Close(comp)
	p.compStore.DeleteComponent(comp.Name)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p.reportInline(ctx, comp, operatorv1.EventType_EVENT_CLOSE, closeErr)
	return closeErr
}

func (p *Processor) reportInline(ctx context.Context, comp compapi.Component, et operatorv1.EventType, opErr error) {
	if p.reporter == nil {
		return
	}
	cond := operatorv1.ResourceConditionStatus_STATUS_SUCCESS
	var reason, message *string
	if opErr != nil {
		cond = operatorv1.ResourceConditionStatus_STATUS_FAILURE
		r := "ERROR"
		m := opErr.Error()
		reason = &r
		message = &m
	}
	if err := p.reporter(ctx, comp, &operatorv1.ResourceResult{
		ResourceType:        operatorv1.ResourceType_RESOURCE_COMPONENT,
		EventType:           et,
		Name:                comp.GetName(),
		Condition:           cond,
		Reason:              reason,
		Message:             message,
		ObservedGeneration:  comp.GetGeneration(),
		LastTransactionTime: timestamppb.New(time.Now()),
	}); err != nil {
		log.Errorf("error reporting component %s result: %s", et, err)
	}
}
<<<<<<< HEAD
=======

func (p *Processor) processComponents(ctx context.Context) error {
	process := func(comp componentsapi.Component) error {
		if comp.Name == "" {
			return nil
		}

		err := p.processComponentAndDependents(ctx, comp)
		if err != nil {
			err = fmt.Errorf("process component %s error: %s", comp.Name, err)
			if !comp.Spec.IgnoreErrors {
				log.Warnf("Error processing component, daprd will exit gracefully: %s", err)
				return err
			}

			log.Errorf("Ignoring error processing component: %s", err)
		}

		return nil
	}

	for comp := range p.pendingComponents {
		err := process(comp)

		p.pendingComponentsWaiting.RUnlock()

		if err != nil {
			return err
		}
	}

	return nil
}

// WaitForEmptyComponentQueue waits for the component queue to be empty.
func (p *Processor) WaitForEmptyComponentQueue() {
	p.pendingComponentsWaiting.Lock()
	defer p.pendingComponentsWaiting.Unlock()
}

func (p *Processor) processComponentAndDependents(ctx context.Context, comp componentsapi.Component) error {
	log.Debug("Loading component: " + comp.LogName())

	res := p.preprocessOneComponent(ctx, &comp)
	if res.unreadyDependency != "" {
		// Dedupe by name: the hot reload reconciler re-creates a parked
		// component on every reconcile (it is not in the component store
		// while parked), which would otherwise grow the parked list and
		// double-init on flush.
		deps := p.pendingComponentDependents[res.unreadyDependency]
		replaced := false
		for i := range deps {
			if deps[i].Name == comp.Name {
				deps[i] = comp
				replaced = true
				break
			}
		}
		if !replaced {
			p.pendingComponentDependents[res.unreadyDependency] = append(deps, comp)
		}
		return nil
	}

	compCategory := p.category(comp)
	if compCategory == "" {
		// the category entered is incorrect, return error
		return fmt.Errorf("incorrect type %s", comp.Spec.Type)
	}

	timeout, err := time.ParseDuration(comp.Spec.InitTimeout)
	if err != nil {
		timeout = defaultComponentInitTimeout
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	err = p.Init(ctx, comp)
	if err != nil {
		log.Errorf("Failed to init component %s: %s", comp.LogName(), err)
		diag.DefaultMonitoring.ComponentInitFailed(comp.Spec.Type, "init", comp.Name)

		return rterrors.NewInit(rterrors.InitComponentFailure, comp.LogName(), err)
	}

	log.Info("Component loaded: " + comp.LogName())
	diag.DefaultMonitoring.ComponentLoaded()

	dependency := componentDependency(compCategory, comp.Name)
	if deps, ok := p.pendingComponentDependents[dependency]; ok {
		delete(p.pendingComponentDependents, dependency)

		for _, dependent := range deps {
			err := p.processComponentAndDependents(ctx, dependent)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

type componentPreprocessRes struct {
	unreadyDependency string
}

func (p *Processor) preprocessOneComponent(ctx context.Context, comp *componentsapi.Component) componentPreprocessRes {
	_, unreadySecretsStore := p.secret.ProcessResource(ctx, comp)
	if unreadySecretsStore != "" {
		return componentPreprocessRes{
			unreadyDependency: componentDependency(components.CategorySecretStore, unreadySecretsStore),
		}
	}

	return componentPreprocessRes{}
}

func (p *Processor) category(comp componentsapi.Component) components.Category {
	for category := range p.managers {
		if strings.HasPrefix(comp.Spec.Type, string(category)+".") {
			return category
		}
	}

	return ""
}

func componentDependency(compCategory components.Category, name string) string {
	return string(compCategory) + ":" + name
}
>>>>>>> upstream/release-1.18
