package main

import (
	"context"
	"fmt"

	"github.com/Zyrexam/Workflow-Engine/internal/engine"
)

func defaultHandlers() map[string]engine.StepFunc {
	return map[string]engine.StepFunc{
		"greet": func(ctx context.Context) error {
			fmt.Println("hello from the workflow engine")
			return nil
		},
		"fetch": func(ctx context.Context) error {
			fmt.Println("pipeline: fetching data")
			return nil
		},
		"transform": func(ctx context.Context) error {
			fmt.Println("pipeline: transforming data")
			return nil
		},
		"save": func(ctx context.Context) error {
			fmt.Println("pipeline: saving result")
			return nil
		},
	}
}