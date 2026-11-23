package ci

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type Config struct {
	SourceRepo     string
	RepositoryURL  string
	TestRepo       string
	Branch         string
	WorkerBin      string
	AgentAddr      string
	OdysseyHost    string
	OdysseyPort    uint
	Database       string
	User           string
	Scenario       string
	Clients        uint
	Duration       uint
	Progress       uint
	DatabaseURL    string
	SpoolDir       string
	OdysseyConfig  string
	OdysseyPIDFile string
	OdysseyLog     string
	StateFile      string
	PollInterval   time.Duration
	BuildTimeout   time.Duration
	StartupTimeout time.Duration
	RetryFailed    bool
}

type state struct {
	Commit string `json:"commit"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

type Monitor struct {
	config Config
	state  state
	cmd    *exec.Cmd
}

func New(config Config) (*Monitor, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	monitor := &Monitor{config: config}
	if err := monitor.loadState(); err != nil {
		return nil, err
	}
	return monitor, nil
}

func (m *Monitor) Run(ctx context.Context) error {
	defer m.stopOdyssey()

	if err := m.runOnce(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "initial benchmark check failed: %v\n", err)
	}

	ticker := time.NewTicker(m.config.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := m.runOnce(ctx); err != nil {
				fmt.Fprintf(os.Stderr, "benchmark check failed: %v\n", err)
			}
		}
	}
}

func (m *Monitor) runOnce(ctx context.Context) error {
	remoteSHA, err := m.syncTestRepo(ctx)
	if err != nil {
		return err
	}
	if remoteSHA == m.state.Commit && (m.state.Status == "succeeded" || (m.state.Status == "failed" && !m.config.RetryFailed)) {
		fmt.Printf("commit %.12s already processed with status %s\n", remoteSHA, m.state.Status)
		return nil
	}

	fmt.Printf("new Odyssey commit detected: %.12s\n", remoteSHA)
	m.state = state{Commit: remoteSHA, Status: "running"}
	if err := m.saveState(); err != nil {
		return err
	}

	if err := m.build(ctx); err != nil {
		return m.fail(remoteSHA, err)
	}
	if err := m.restartOdyssey(ctx); err != nil {
		return m.fail(remoteSHA, err)
	}
	if err := m.runWorker(ctx, remoteSHA); err != nil {
		return m.fail(remoteSHA, err)
	}

	m.state.Status = "succeeded"
	m.state.Error = ""
	if err := m.saveState(); err != nil {
		return err
	}
	fmt.Printf("benchmark for %.12s succeeded\n", remoteSHA)
	return nil
}

func (m *Monitor) syncTestRepo(ctx context.Context) (string, error) {
	repositoryURL := m.config.RepositoryURL
	if repositoryURL == "" {
		var err error
		repositoryURL, err = gitOutput(ctx, m.config.SourceRepo, "remote", "get-url", "origin")
		if err != nil {
			return "", fmt.Errorf("get source repository URL: %w", err)
		}
	}
	if _, err := os.Stat(filepath.Join(m.config.TestRepo, ".git")); errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(filepath.Dir(m.config.TestRepo), 0o750); err != nil {
			return "", fmt.Errorf("create test repository parent: %w", err)
		}
		if err := runGitCommand(ctx, filepath.Dir(m.config.TestRepo), "clone", "--branch", m.config.Branch, "--single-branch", repositoryURL, m.config.TestRepo); err != nil {
			return "", fmt.Errorf("clone test repository: %w", err)
		}
	} else if err != nil {
		return "", fmt.Errorf("inspect test repository: %w", err)
	}
	if err := runGitCommand(ctx, m.config.TestRepo, "remote", "set-url", "origin", repositoryURL); err != nil {
		return "", fmt.Errorf("set test repository origin: %w", err)
	}

	if err := runGitCommand(ctx, m.config.TestRepo, "fetch", "origin", m.config.Branch, "--prune"); err != nil {
		return "", fmt.Errorf("fetch %s: %w", m.config.Branch, err)
	}
	remoteSHA, err := gitOutput(ctx, m.config.TestRepo, "rev-parse", "origin/"+m.config.Branch)
	if err != nil {
		return "", fmt.Errorf("resolve origin/%s: %w", m.config.Branch, err)
	}
	if err := runGitCommand(ctx, m.config.TestRepo, "checkout", "-B", m.config.Branch, "origin/"+m.config.Branch); err != nil {
		return "", fmt.Errorf("checkout %s: %w", m.config.Branch, err)
	}
	if err := runGitCommand(ctx, m.config.TestRepo, "reset", "--hard", "origin/"+m.config.Branch); err != nil {
		return "", fmt.Errorf("reset test repository: %w", err)
	}
	if err := runGitCommand(ctx, m.config.TestRepo, "clean", "-fdx"); err != nil {
		return "", fmt.Errorf("clean test repository: %w", err)
	}
	return remoteSHA, nil
}

func (m *Monitor) build(parent context.Context) error {
	ctx, cancel := context.WithTimeout(parent, m.config.BuildTimeout)
	defer cancel()
	fmt.Printf("building Odyssey in %s\n", m.config.TestRepo)
	if err := runCommand(ctx, m.config.TestRepo, "make", "build_release"); err != nil {
		return fmt.Errorf("build Odyssey: %w", err)
	}
	return nil
}

func (m *Monitor) restartOdyssey(parent context.Context) error {
	m.stopOdyssey()
	if err := stopPIDFile(m.config.OdysseyPIDFile); err != nil {
		return fmt.Errorf("stop previous Odyssey: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(m.config.OdysseyLog), 0o750); err != nil {
		return fmt.Errorf("create Odyssey log directory: %w", err)
	}
	logFile, err := os.OpenFile(m.config.OdysseyLog, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return fmt.Errorf("open Odyssey log: %w", err)
	}
	cmd := exec.Command(m.config.OdysseyBinary(), m.config.OdysseyConfig)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return fmt.Errorf("start Odyssey: %w", err)
	}
	m.cmd = cmd
	go func() {
		_ = cmd.Wait()
		_ = logFile.Close()
	}()

	ctx, cancel := context.WithTimeout(parent, m.config.StartupTimeout)
	defer cancel()
	address := net.JoinHostPort(m.config.OdysseyHost, strconv.FormatUint(uint64(m.config.OdysseyPort), 10))
	for {
		connection, dialErr := net.DialTimeout("tcp", address, 500*time.Millisecond)
		if dialErr == nil {
			_ = connection.Close()
			fmt.Printf("Odyssey is ready on %s\n", address)
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for Odyssey on %s: %w", address, ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (m *Monitor) runWorker(ctx context.Context, commit string) error {
	args := []string{
		"-agent", m.config.AgentAddr,
		"-host", m.config.OdysseyHost,
		"-port", strconv.FormatUint(uint64(m.config.OdysseyPort), 10),
		"-db", m.config.Database,
		"-user", m.config.User,
		"-clients", strconv.FormatUint(uint64(m.config.Clients), 10),
		"-duration", strconv.FormatUint(uint64(m.config.Duration), 10),
		"-progress", strconv.FormatUint(uint64(m.config.Progress), 10),
		"-scenario", m.config.Scenario,
		"-commit-hash", commit,
	}
	worker := exec.CommandContext(ctx, m.config.WorkerBin, args...)
	worker.Env = append(os.Environ(),
		"RESULTS_DATABASE_URL="+m.config.DatabaseURL,
		"RESULTS_SPOOL_DIR="+m.config.SpoolDir,
	)
	worker.Stdout = os.Stdout
	worker.Stderr = os.Stderr
	fmt.Printf("running benchmark for %.12s\n", commit)
	if err := worker.Run(); err != nil {
		return fmt.Errorf("run benchmark worker: %w", err)
	}
	return nil
}

func (m *Monitor) fail(commit string, cause error) error {
	m.state = state{Commit: commit, Status: "failed", Error: cause.Error()}
	if err := m.saveState(); err != nil {
		return fmt.Errorf("%v; save failed state: %w", cause, err)
	}
	return cause
}

func (m *Monitor) loadState() error {
	data, err := os.ReadFile(m.config.StateFile)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read CI state: %w", err)
	}
	if err := json.Unmarshal(data, &m.state); err != nil {
		return fmt.Errorf("decode CI state: %w", err)
	}
	return nil
}

func (m *Monitor) saveState() error {
	if err := os.MkdirAll(filepath.Dir(m.config.StateFile), 0o750); err != nil {
		return fmt.Errorf("create CI state directory: %w", err)
	}
	data, err := json.MarshalIndent(m.state, "", "  ")
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(m.config.StateFile), ".ci-state-*")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryName)
	}()
	if _, err := temporary.Write(data); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryName, m.config.StateFile)
}

func (m *Monitor) stopOdyssey() {
	if m.cmd == nil || m.cmd.Process == nil {
		return
	}
	_ = m.cmd.Process.Signal(syscall.SIGTERM)
	_ = m.cmd.Process.Release()
	m.cmd = nil
}

func (m Config) OdysseyBinary() string {
	return filepath.Join(m.TestRepo, "build", "sources", "odyssey")
}

func validateConfig(config Config) error {
	fields := []struct {
		name  string
		value string
	}{
		{"test repository", config.TestRepo},
		{"worker binary", config.WorkerBin},
		{"agent address", config.AgentAddr},
		{"Odyssey host", config.OdysseyHost},
		{"database", config.Database},
		{"user", config.User},
		{"scenario", config.Scenario},
		{"results database URL", config.DatabaseURL},
		{"spool directory", config.SpoolDir},
		{"Odyssey config", config.OdysseyConfig},
		{"Odyssey PID file", config.OdysseyPIDFile},
		{"Odyssey log", config.OdysseyLog},
		{"state file", config.StateFile},
	}
	if strings.TrimSpace(config.RepositoryURL) == "" && strings.TrimSpace(config.SourceRepo) == "" {
		return errors.New("repository URL or source repository is required")
	}
	for _, field := range fields {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("%s is required", field.name)
		}
	}
	if config.Branch == "" {
		return errors.New("branch is required")
	}
	if config.OdysseyPort == 0 || config.Clients == 0 || config.Duration == 0 || config.Progress == 0 {
		return errors.New("Odyssey port, clients, duration, and progress must be positive")
	}
	if config.PollInterval <= 0 || config.BuildTimeout <= 0 || config.StartupTimeout <= 0 {
		return errors.New("poll, build, and startup timeouts must be positive")
	}
	if config.Progress > config.Duration {
		return errors.New("progress must not be greater than duration")
	}
	return nil
}

func gitOutput(ctx context.Context, directory string, args ...string) (string, error) {
	command := exec.CommandContext(ctx, "git", args...)
	command.Dir = directory
	output, err := command.Output()
	if err != nil {
		return "", commandError(command, output, err)
	}
	return strings.TrimSpace(string(output)), nil
}

func runCommand(ctx context.Context, directory string, args ...string) error {
	command := exec.CommandContext(ctx, args[0], args[1:]...)
	command.Dir = directory
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	return command.Run()
}

func runGitCommand(ctx context.Context, directory string, args ...string) error {
	gitArgs := append([]string{"git"}, args...)
	return runCommand(ctx, directory, gitArgs...)
}

func commandError(command *exec.Cmd, output []byte, err error) error {
	if exitError, ok := err.(*exec.ExitError); ok {
		if len(exitError.Stderr) > 0 {
			return fmt.Errorf("%s: %w: %s", strings.Join(command.Args, " "), err, strings.TrimSpace(string(exitError.Stderr)))
		}
	}
	if len(output) > 0 {
		return fmt.Errorf("%s: %w: %s", strings.Join(command.Args, " "), err, strings.TrimSpace(string(output)))
	}
	return fmt.Errorf("%s: %w", strings.Join(command.Args, " "), err)
}

func stopPIDFile(path string) error {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 {
		return fmt.Errorf("invalid PID file %q", path)
	}
	process, err := os.FindProcess(pid)
	if err == nil {
		if err := process.Signal(syscall.SIGTERM); err != nil &&
			!errors.Is(err, os.ErrProcessDone) && !errors.Is(err, syscall.ESRCH) {
			return fmt.Errorf("terminate PID %d: %w", pid, err)
		}
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			_ = os.Remove(path)
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("kill PID %d: %w", pid, err)
	}
	_ = os.Remove(path)
	return nil
}
