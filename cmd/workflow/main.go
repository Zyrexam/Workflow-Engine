package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/Zyrexam/Workflow-Engine/internal/engine"
	"github.com/Zyrexam/Workflow-Engine/internal/store"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	fs := flag.NewFlagSet("workflow", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	dbPath := fs.String("db", "db/state.db", "path to the SQLite state database")
	configPath := fs.String("config", "", "path to a JSON workflow config file")

	if err := fs.Parse(os.Args[1:]); err != nil {
		os.Exit(2)
	}
	args := fs.Args()
	if len(args) < 1 {
		usage()
	}

	cmd, rest := args[0], args[1:]
	var err error
	switch cmd {
	case "run":
		err = requireArgs(rest, 1, func(arg string) error {
			return run(context.Background(), *dbPath, *configPath, arg)
		})
	case "status":
		err = requireArgs(rest, 1, func(arg string) error {
			return status(*dbPath, arg)
		})
	case "resume":
		err = requireArgs(rest, 1, func(arg string) error {
			return resume(context.Background(), *dbPath, *configPath, arg)
		})
	default:
		usage()
	}
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  workflow [-db path] [-config file] run <workflow-name>
  workflow [-db path] [-config file] resume <run-id>
  workflow [-db path] status <run-id>`)
	fmt.Fprintln(os.Stderr, "(flags must precede the subcommand)")
	os.Exit(2)
}

func requireArgs(args []string, n int, fn func(arg string) error) error {
	if len(args) != n {
		usage()
	}
	for _, arg := range args {
		if err := fn(arg); err != nil {
			return err
		}
	}
	return nil
}

func run(ctx context.Context, dbPath, configPath, workflowName string) error {
	reg := engine.NewRegistry()
	if err := registerWorkflows(reg); err != nil {
		return err
	}
	if configPath != "" {
		wfs, err := engine.LoadWorkflows(configPath, defaultHandlers())
		if err != nil {
			return err
		}
		for _, wf := range wfs {
			if err := reg.Register(wf); err != nil {
				return err
			}
		}
	}

	st, err := store.Open(dbPath)
	if err != nil {
		return err
	}
	defer st.Close()

	w := &engine.Worker{Store: st, Reg: reg}
	runID, err := w.Execute(ctx, workflowName)
	if err != nil {
		return err
	}
	r, err := st.GetRun(runID)
	if err != nil {
		return err
	}
	printRun(r)
	return nil
}

func resume(ctx context.Context, dbPath, configPath, runIDArg string) error {
	runID, err := engine.ParseRunID(runIDArg)
	if err != nil {
		return err
	}
	reg := engine.NewRegistry()
	if err := registerWorkflows(reg); err != nil {
		return err
	}
	if configPath != "" {
		wfs, err := engine.LoadWorkflows(configPath, defaultHandlers())
		if err != nil {
			return err
		}
		for _, wf := range wfs {
			if err := reg.Register(wf); err != nil {
				return err
			}
		}
	}
	st, err := store.Open(dbPath)
	if err != nil {
		return err
	}
	defer st.Close()

	w := &engine.Worker{Store: st, Reg: reg}
	runID, err = w.Resume(ctx, runID)
	if errors.Is(err, engine.ErrRunCompleted) {
		fmt.Printf("Run %d already completed; nothing to resume\n", runID)
		r, err := st.GetRun(runID)
		if err != nil {
			return err
		}
		printRun(r)
		return nil
	}
	if err != nil {
		return err
	}
	r, err := st.GetRun(runID)
	if err != nil {
		return err
	}
	printRun(r)
	return nil
}

func status(dbPath, runIDArg string) error {
	runID, err := engine.ParseRunID(runIDArg)
	if err != nil {
		return err
	}
	st, err := store.Open(dbPath)
	if err != nil {
		return err
	}
	defer st.Close()

	r, err := st.GetRun(runID)
	if err != nil {
		return err
	}
	printRun(r)
	return nil
}

func printRun(r store.Run) {
	fmt.Printf("Run %d (%s): %s\n", r.ID, r.Workflow, r.Status)
	for _, st := range r.Steps {
		line := fmt.Sprintf("  [%2d] %-12s %s", st.Index, st.Name, st.Status)
		if st.Attempts > 0 {
			line += fmt.Sprintf(" (attempts=%d)", st.Attempts)
		}
		fmt.Println(line)
		if st.LastError != "" {
			fmt.Printf("       last error: %s\n", st.LastError)
		}
	}
}
