package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Zyrexam/Workflow-Engine/internal/engine"
	"github.com/Zyrexam/Workflow-Engine/internal/store"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	fs := flag.NewFlagSet("workflow", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	dbPath := fs.String("db", "db/state.db", "path to the SQLite state database")
	configPath := fs.String("config", "", "path to a JSON workflow config file")
	timeout := fs.Duration("timeout", 0, "overall run deadline (e.g. 30s); 0 = none")

	if err := fs.Parse(os.Args[1:]); err != nil {
		os.Exit(2)
	}
	args := fs.Args()
	if len(args) < 1 {
		usage()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	cmd, rest := args[0], args[1:]
	var err error
	switch cmd {
	case "run":
		err = requireArgs(rest, 1, func(arg string) error {
			return run(ctx, *dbPath, *configPath, *timeout, arg)
		})
	case "status":
		err = requireArgs(rest, 1, func(arg string) error {
			return status(*dbPath, arg)
		})
	case "list":
		lfs := flag.NewFlagSet("list", flag.ContinueOnError)
		lfs.SetOutput(os.Stderr)
		lStatus := lfs.String("status", "", "filter by run status")
		lLimit := lfs.Int("limit", 50, "max runs")
		lOffset := lfs.Int("offset", 0, "offset")
		if perr := lfs.Parse(rest); perr != nil {
			os.Exit(2)
		}
		if lfs.NArg() != 0 {
			usage()
		}
		err = list(*dbPath, *lStatus, *lLimit, *lOffset)
	case "resume":
		err = requireArgs(rest, 1, func(arg string) error {
			return resume(ctx, *dbPath, *configPath, *timeout, arg)
		})
	default:
		usage()
	}
	if err != nil {
		if errors.Is(err, errUsage) || errors.Is(err, engine.ErrBadRunID) {
			usage()
		}
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  workflow [-db path] [-config file] run <workflow-name>
  workflow [-db path] [-config file] resume <run-id>
  workflow [-db path] status <run-id>
  workflow [-db path] list [-status S] [-limit N] [-offset N]`)
	fmt.Fprintln(os.Stderr, "(flags must precede the subcommand)")
	os.Exit(2)
}

var errUsage = errors.New("usage error")

func requireArgs(args []string, n int, fn func(arg string) error) error {
	if len(args) != n {
		return errUsage
	}
	for _, arg := range args {
		if err := fn(arg); err != nil {
			return err
		}
	}
	return nil
}

func run(ctx context.Context, dbPath, configPath string, timeout time.Duration, workflowName string) error {
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

	w := &engine.Worker{Store: st, Reg: reg, RunTimeout: timeout}
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

func resume(ctx context.Context, dbPath, configPath string, timeout time.Duration, runIDArg string) error {
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

	w := &engine.Worker{Store: st, Reg: reg, RunTimeout: timeout}
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

func list(dbPath, statusFilter string, limit, offset int) error {
	if statusFilter != "" {
		switch statusFilter {
		case "pending", "running", "completed", "failed":
		default:
			return fmt.Errorf("unknown status %q (want pending/running/completed/failed)", statusFilter)
		}
	}
	st, err := store.Open(dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	runs, err := st.ListRuns(statusFilter, limit, offset)
	if err != nil {
		return err
	}
	for _, r := range runs {
		fmt.Printf("Run %d (%s): %s  [%s]\n", r.ID, r.Workflow, r.Status, r.CreatedAt)
	}
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
		line := fmt.Sprintf("  [%2d] %-16s %s", st.Index, st.Name, st.Status)
		if st.Attempts > 0 {
			line += fmt.Sprintf(" (attempts=%d)", st.Attempts)
		}
		fmt.Println(line)
		if st.LastError != "" {
			fmt.Printf("       last error: %s\n", st.LastError)
		}
	}
}
