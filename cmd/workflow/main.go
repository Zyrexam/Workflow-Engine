package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/Zyrexam/workflowengine/internal/engine"
	"github.com/Zyrexam/workflowengine/internal/store"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	fs := flag.NewFlagSet("workflow", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	dbPath := fs.String("db", "db/state.db", "path to the SQLite state database")

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
			return run(context.Background(), *dbPath, arg)
		})
	case "status":
		err = requireArgs(rest, 1, func(arg string) error {
			return status(*dbPath, arg)
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
  workflow [-db path] run <workflow-name>
  workflow [-db path] status <run-id>`)
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

func run(ctx context.Context, dbPath, workflowName string) error {
	reg := engine.NewRegistry()
	if err := registerWorkflows(reg); err != nil {
		return err
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
		fmt.Printf("  [%2d] %-12s %s\n", st.Index, st.Name, st.Status)
	}
}
