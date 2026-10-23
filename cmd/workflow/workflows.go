package main

import (
	"context"
	"fmt"

	"github.com/Zyrexam/Workflow-Engine/internal/engine"
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
	if err := reg.Register(engine.Workflow{
		Name: "pipeline",
		Steps: []engine.Step{
			{Name: "fetch", Fn: func(ctx context.Context) error {
				fmt.Println("pipeline: fetching data")
				return nil
			}},
			{Name: "transform", Fn: func(ctx context.Context) error {
				fmt.Println("pipeline: transforming data")
				return nil
			}},
			{Name: "save", Fn: func(ctx context.Context) error {
				fmt.Println("pipeline: saving result")
				return nil
			}},
		},
	}); err != nil {
		return fmt.Errorf("register workflows: %w", err)
	}
	return nil
}
