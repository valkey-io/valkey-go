package valkey

import (
	"errors"
	"testing"
	"time"
)

func TestClassifyASMMigrationPhase(t *testing.T) {
	tests := []struct {
		name      string
		operation string
		state     string
		want      asmMigrationPhase
	}{
		{"export copying", "EXPORT", "replicating", asmPhaseActive},
		{"export approaching finalization", "EXPORT", "waiting-to-pause", asmPhaseApproachingFinalization},
		{"import waiting for pause", "IMPORT", "waiting-for-paused", asmPhaseApproachingFinalization},
		{"export paused", "EXPORT", "failover-paused", asmPhaseFinalizing},
		{"export granted", "EXPORT", "failover-granted", asmPhaseFinalizing},
		{"import requested", "IMPORT", "failover-requested", asmPhaseFinalizing},
		{"import granted", "IMPORT", "failover-granted", asmPhaseFinalizing},
		{"import cleanup", "IMPORT", "cleaning-up", asmPhaseFinalizing},
		{"import on primary", "IMPORT", "occurring-on-primary", asmPhaseFinalizing},
		{"success", "EXPORT", "success", asmPhaseTerminal},
		{"failed", "EXPORT", "failed", asmPhaseTerminal},
		{"cancelled", "IMPORT", "cancelled", asmPhaseTerminal},
		{"unknown state stays active", "EXPORT", "future-state", asmPhaseActive},
		{"case insensitive", "export", "WAITING-TO-PAUSE", asmPhaseApproachingFinalization},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyASMMigrationPhase(tt.operation, tt.state); got != tt.want {
				t.Fatalf("classifyASMMigrationPhase(%q, %q) = %v, want %v", tt.operation, tt.state, got, tt.want)
			}
		})
	}
}

func TestIsUnsupportedASMMigrationError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"NOPERM", errors.New("NOPERM this user has no permissions"), true},
		{"unknown command", errors.New("ERR unknown command 'CLUSTER GETSLOTMIGRATIONS'"), true},
		{"unknown subcommand", errors.New("ERR unknown subcommand"), true},
		{"not allowed", errors.New("ERR command is not allowed"), true},
		{"transient timeout", errors.New("i/o timeout"), false},
		{"connection reset", errors.New("connection reset by peer"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isUnsupportedASMMigrationError(tt.err); got != tt.want {
				t.Fatalf("isUnsupportedASMMigrationError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func newTestASMMigrationMonitor() *atomicSlotMigrationMonitor {
	return &atomicSlotMigrationMonitor{
		migrationStates:  make(map[string]asmMigrationObservation),
		pendingRefreshes: make(map[string]asmMigrationPhase),
	}
}

func TestObserveMigrationDetectsPhaseTransitionsOnlyOnce(t *testing.T) {
	monitor := newTestASMMigrationMonitor()
	now := time.Now()

	record := asmMigrationRecord{name: "migration-1", operation: "EXPORT", state: "replicating"}
	if got := monitor.observeMigration(record, now); got {
		t.Fatal("active phase must not request a topology refresh")
	}
	if len(monitor.pendingRefreshes) != 0 {
		t.Fatalf("pending refreshes = %v, want none", monitor.pendingRefreshes)
	}

	record.state = "waiting-to-pause"
	if got := monitor.observeMigration(record, now.Add(time.Millisecond)); !got {
		t.Fatal("first observation of approaching finalization should request refresh")
	}
	if got := monitor.observeMigration(record, now.Add(2*time.Millisecond)); got {
		t.Fatal("repeated phase observation must not request another refresh")
	}

	record.state = "failover-granted"
	if got := monitor.observeMigration(record, now.Add(3*time.Millisecond)); !got {
		t.Fatal("transition into finalizing should request refresh")
	}
	if got := monitor.observeMigration(record, now.Add(4*time.Millisecond)); got {
		t.Fatal("repeated finalizing phase must not request another refresh")
	}

	record.state = "success"
	if got := monitor.observeMigration(record, now.Add(5*time.Millisecond)); !got {
		t.Fatal("transition into terminal phase should request refresh")
	}
	if got := monitor.observeMigration(record, now.Add(6*time.Millisecond)); got {
		t.Fatal("repeated terminal observation must not request another refresh")
	}

	if got := monitor.pendingRefreshes[record.name]; got != asmPhaseTerminal {
		t.Fatalf("pending terminal phase = %v, want %v", got, asmPhaseTerminal)
	}
}

func TestObserveMigrationDetectsTerminalRecordFirstSeenAfterCompletion(t *testing.T) {
	monitor := newTestASMMigrationMonitor()
	now := time.Now()
	record := asmMigrationRecord{name: "fast-migration", operation: "EXPORT", state: "success"}

	// This represents a migration that completed between discovery polls, so
	// the client first sees it in a terminal state. No server/client clock
	// comparison is required.
	if got := monitor.observeMigration(record, now); !got {
		t.Fatal("first observation of a terminal record should request refresh")
	}
	if got := monitor.observeMigration(record, now.Add(time.Second)); got {
		t.Fatal("repeated terminal record should not request another refresh")
	}
}

func TestObserveMigrationDoesNotRegressOnStaleNodeState(t *testing.T) {
	monitor := newTestASMMigrationMonitor()
	now := time.Now()

	terminal := asmMigrationRecord{name: "migration-2", operation: "EXPORT", state: "success"}
	if !monitor.observeMigration(terminal, now) {
		t.Fatal("first terminal observation should request refresh")
	}

	// A lagging source/target response must not downgrade the remembered phase
	// and trigger a second refresh.
	stale := asmMigrationRecord{name: "migration-2", operation: "IMPORT", state: "replicating"}
	if monitor.observeMigration(stale, now.Add(time.Millisecond)) {
		t.Fatal("stale active observation must not request another refresh")
	}
	if got := monitor.migrationStates["migration-2"].phase; got != asmPhaseTerminal {
		t.Fatalf("remembered phase = %v, want terminal (%v)", got, asmPhaseTerminal)
	}
}
