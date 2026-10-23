package main

import (
	"context"
	"fmt"

	"github.com/Zyrexam/workflowengine/internal/engine"
)

func registerWorkflows(reg *engine.Registry) error {
	if err := reg.Register(engine.Workflow{
		Name: "hello",
		Steps: []engine.Step{
			{Name: "greet", Fn: func(ctx context.Context) error {
				fmt.Println("hello from the workflow engine")
				return nil
			}},
		},
	}); err != nil {
		return fmt.Errorf("register workflows: %w", err)
	}
	return nil
}
