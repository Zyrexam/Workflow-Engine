package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestCrashRecoveryEndToEnd(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "state.db")
	bin := filepath.Join(tmp, "workflow.exe")

	build := exec.Command("go", "build", "-o", bin, ".")
	build.Dir = "."
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build failed: %v\n%s", err, out)
	}

	env := append(os.Environ(), "CRASH_DIR="+tmp)

	// Start run; it will block in "wait" step.
	proc := exec.Command(bin, "-db", dbPath, "run", "crashy")
	proc.Env = env
	if err := proc.Start(); err != nil {
		t.Fatal(err)
	}

	// Wait for "before" marker (step0 completed).
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(tmp, "before.txt")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			proc.Process.Kill()
			t.Fatal("step0 did not complete in time")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Kill mid-run.
	if err := proc.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	proc.Wait()

	// before.txt should have exactly one "x" (step0 ran once).
	data, _ := os.ReadFile(filepath.Join(tmp, "before.txt"))
	if string(data) != "x" {
		t.Fatalf("step0 ran %d times, want 1: %q", len(data), data)
	}

	// Allow resume to complete by creating go.txt.
	os.WriteFile(filepath.Join(tmp, "go.txt"), []byte("go"), 0o644)

	// Resume.
	res := exec.Command(bin, "-db", dbPath, "resume", "1")
	res.Env = env
	out, err := res.CombinedOutput()
	if err != nil {
		t.Fatalf("resume failed: %v\n%s", err, out)
	}

	// after.txt should exist.
	if _, err := os.Stat(filepath.Join(tmp, "after.txt")); err != nil {
		t.Fatalf("run did not complete: %v\n%s", err, out)
	}
	// before.txt still one "x" (step0 not re-run).
	data2, _ := os.ReadFile(filepath.Join(tmp, "before.txt"))
	if string(data2) != "x" {
		t.Fatalf("step0 re-ran on resume: %q", data2)
	}
}