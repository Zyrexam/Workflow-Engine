package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type stepConfig struct {
	Name    string
	Timeout int
}

func (s *stepConfig) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		s.Name = str
		return nil
	}
	type alias struct {
		Name    string `json:"name"`
		Timeout int    `json:"timeout_seconds"`
	}
	var a alias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	s.Name = a.Name
	s.Timeout = a.Timeout
	return nil
}

type workflowConfig struct {
	Workflows []struct {
		Name    string      `json:"name"`
		Steps   []stepConfig `json:"steps"`
		Timeout int         `json:"timeout_seconds"`
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
		for _, sc := range w.Steps {
			fn, ok := handlers[sc.Name]
			if !ok {
				return nil, fmt.Errorf("workflow %q references unknown step handler %q", w.Name, sc.Name)
			}
			st := Step{Name: sc.Name, Fn: fn}
			if sc.Timeout > 0 {
				st.Timeout = time.Duration(sc.Timeout) * time.Second
			}
			steps = append(steps, st)
		}
	
		wf := Workflow{Name: w.Name, Steps: steps}
		if w.Timeout > 0 {
			wf.Timeout = time.Duration(w.Timeout) * time.Second
		}
	
		out = append(out, wf)
	}
	return out, nil
}