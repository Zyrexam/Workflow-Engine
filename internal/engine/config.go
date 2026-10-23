package engine

import (
	"encoding/json"
	"fmt"
	"os"
)

type workflowConfig struct {
	Workflows []struct {
		Name  string   `json:"name"`
		Steps []string `json:"steps"`
	} `json:"workflows"`
}

func LoadWorkflows(path string, handlers map[string]StepFunc) ([]Workflow, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read workflow config %q: %w", path, err)
	}
	var cfg workflowConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse workflow config %q: %w", path, err)
	}
	out := make([]Workflow, 0, len(cfg.Workflows))
	for _, w := range cfg.Workflows {
		if w.Name == "" {
			return nil, fmt.Errorf("workflow with empty name in %q", path)
		}
		if len(w.Steps) == 0 {
			return nil, fmt.Errorf("workflow %q has no steps", w.Name)
		}
		steps := make([]Step, 0, len(w.Steps))
		for _, h := range w.Steps {
			fn, ok := handlers[h]
			if !ok {
				return nil, fmt.Errorf("workflow %q references unknown step handler %q", w.Name, h)
			}
			steps = append(steps, Step{Name: h, Fn: fn})
		}
		out = append(out, Workflow{Name: w.Name, Steps: steps})
	}
	return out, nil
}