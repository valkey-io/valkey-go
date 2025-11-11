package valkey

import (
	"testing"

	"github.com/valkey-io/valkey-go/internal/cmds"
)

// TestSlotMigrationCommands tests the new atomic slot migration command builders
func TestSlotMigrationCommands(t *testing.T) {
	builder := cmds.NewBuilder(cmds.NoSlot)

	t.Run("CLUSTER CANCELMIGRATION ALL", func(t *testing.T) {
		cmd := builder.ClusterCancelmigration().All().Build()
		expected := []string{"CLUSTER", "CANCELMIGRATION", "ALL"}
		actual := cmd.Commands()

		if len(actual) != len(expected) {
			t.Errorf("Expected %d commands, got %d", len(expected), len(actual))
		}

		for i, exp := range expected {
			if actual[i] != exp {
				t.Errorf("Command[%d]: expected %q, got %q", i, exp, actual[i])
			}
		}
	})

	t.Run("CLUSTER GETSLOTMIGRATIONS", func(t *testing.T) {
		cmd := builder.ClusterGetslotmigrations().Build()
		expected := []string{"CLUSTER", "GETSLOTMIGRATIONS"}
		actual := cmd.Commands()

		if len(actual) != len(expected) {
			t.Errorf("Expected %d commands, got %d", len(expected), len(actual))
		}

		for i, exp := range expected {
			if actual[i] != exp {
				t.Errorf("Command[%d]: expected %q, got %q", i, exp, actual[i])
			}
		}
	})

	t.Run("CLUSTER MIGRATESLOTS single range", func(t *testing.T) {
		cmd := builder.ClusterMigrateslots().
			Slotsrange().StartSlot(0).EndSlot(100).
			Node().NodeId("abc123def456").
			Build()
		expected := []string{"CLUSTER", "MIGRATESLOTS", "SLOTSRANGE", "0", "100", "NODE", "abc123def456"}
		actual := cmd.Commands()

		if len(actual) != len(expected) {
			t.Errorf("Expected %d commands, got %d", len(expected), len(actual))
		}

		for i, exp := range expected {
			if actual[i] != exp {
				t.Errorf("Command[%d]: expected %q, got %q", i, exp, actual[i])
			}
		}
	})

	t.Run("CLUSTER MIGRATESLOTS multiple ranges", func(t *testing.T) {
		cmd := builder.ClusterMigrateslots().
			Slotsrange().StartSlot(0).EndSlot(100).
			Node().NodeId("node1abc").
			Slotsrange().StartSlot(200).EndSlot(300).
			Node().NodeId("node2def").
			Build()
		expected := []string{
			"CLUSTER", "MIGRATESLOTS",
			"SLOTSRANGE", "0", "100", "NODE", "node1abc",
			"SLOTSRANGE", "200", "300", "NODE", "node2def",
		}
		actual := cmd.Commands()

		if len(actual) != len(expected) {
			t.Errorf("Expected %d commands, got %d", len(expected), len(actual))
		}

		for i, exp := range expected {
			if actual[i] != exp {
				t.Errorf("Command[%d]: expected %q, got %q", i, exp, actual[i])
			}
		}
	})
}
