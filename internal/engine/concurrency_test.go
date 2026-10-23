package engine

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

func TestRegistryConcurrent(t *testing.T) {
	r := NewRegistry()
	var wg sync.WaitGroup
	var conflicts atomic.Int64
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := r.Register(Workflow{Name: "wf", Steps: []Step{{Name: "s", Fn: func(ctx context.Context) error { return nil }}}}); err != nil {
				conflicts.Add(1)
			}
			r.Get("wf")
			r.List()
		}()
	}
	wg.Wait()
	if conflicts.Load() != 49 {
		t.Fatalf("expected 49 conflicts, got %d", conflicts.Load())
	}
}