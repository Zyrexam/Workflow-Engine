package main

import (
	"context"
	"fmt"
	"os"
	"time"

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
	if err := reg.Register(engine.Workflow{
		Name: "crashy",
		Steps: []engine.Step{
			{Name: "before", Fn: func(ctx context.Context) error {
				dir := os.Getenv("CRASH_DIR")
				if dir == "" {
					dir = "."
				}
				f, _ := os.OpenFile(dir+"/before.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
				if f != nil {
					f.WriteString("x")
					f.Close()
				}
				return nil
			}},
			{Name: "wait", Fn: func(ctx context.Context) error {
				dir := os.Getenv("CRASH_DIR")
				if dir == "" {
					dir = "."
				}
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
					}
					if _, err := os.Stat(dir + "/go.txt"); err == nil {
						return nil
					}
					time.Sleep(50 * time.Millisecond)
				}
			}},
			{Name: "after", Fn: func(ctx context.Context) error {
				dir := os.Getenv("CRASH_DIR")
				if dir == "" {
					dir = "."
				}
				os.WriteFile(dir+"/after.txt", []byte("done"), 0o644)
				return nil
			}},
		},
	}); err != nil {
		return fmt.Errorf("register workflows: %w", err)
	}
	return nil
}
