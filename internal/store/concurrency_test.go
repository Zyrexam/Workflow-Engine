package store

import (
	"sync"
	"testing"
)

func TestStoreConcurrent(t *testing.T) {
	s := openTestStore(t)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id, err := s.CreateRun("wf", []string{"a", "b"})
			if err != nil {
				t.Errorf("create: %v", err)
				return
			}
			_ = s.SetStepStatus(id, 0, StepStatusRunning)
			_ = s.SetStepStatus(id, 0, StepStatusCompleted)
			if _, err := s.GetRun(id); err != nil {
				t.Errorf("get: %v", err)
			}
		}(i)
	}
	wg.Wait()
}