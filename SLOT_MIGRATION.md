# Atomic Slot Migration Commands

This document describes the atomic slot migration commands introduced in Valkey 9.0, as implemented in [valkey-io/valkey#1949](https://github.com/valkey-io/valkey/pull/1949).

## Overview

Atomic slot migration provides a new family of commands for migrating hash slots between cluster nodes via replication. The procedure is driven by the source node, which pushes an AOF-formatted snapshot of the slots to the target, followed by a replication stream of changes on that slot.

## Commands

### CLUSTER CANCELMIGRATION ALL

Cancel all active slot migration operations.

**Usage:**

```go
client := valkey.NewClient(valkey.ClientOption{
    InitAddress: []string{"127.0.0.1:7000"},
})

// Cancel all slot migrations
result := client.Do(ctx, 
    client.B().ClusterCancelmigration().All().Build(),
)
```

### CLUSTER GETSLOTMIGRATIONS

Get a list of recent slot migration jobs, including both active and recently completed migrations.

**Usage:**

```go
// Get slot migration status
result := client.Do(ctx,
    client.B().ClusterGetslotmigrations().Build(),
)
```

### CLUSTER MIGRATESLOTS

Begin migrating hash slots to target nodes using atomic slot migration. This command supports migrating multiple slot ranges to different nodes in a single command.

**Command Structure:**

```
CLUSTER MIGRATESLOTS SLOTSRANGE start end NODE node-id [SLOTSRANGE start end NODE node-id ...]
```

**Usage - Single Slot Range:**

```go
// Migrate slots 0-100 to a target node
result := client.Do(ctx,
    client.B().ClusterMigrateslots().
        Slotsrange().StartSlot(0).EndSlot(100).
        Node().NodeId("abc123def456...").
        Build(),
)
```

**Usage - Multiple Slot Ranges:**

```go
// Migrate slots 0-100 to node1 and slots 200-300 to node2
result := client.Do(ctx,
    client.B().ClusterMigrateslots().
        Slotsrange().StartSlot(0).EndSlot(100).
        Node().NodeId("node1abc123...").
        Slotsrange().StartSlot(200).EndSlot(300).
        Node().NodeId("node2def456...").
        Build(),
)
```

## Migration Process

The atomic slot migration follows this high-level flow:

1. **Establish**: Source node connects to target and establishes a replication connection
2. **Snapshot**: Source creates an AOF snapshot of the slots being migrated
3. **Stream**: Source streams ongoing changes to the target during migration
4. **Pause**: Source pauses writes when ready for failover
5. **Failover**: Cluster topology is updated to transfer slot ownership
6. **Complete**: Migration job completes successfully

## Error Handling

Migrations can fail or be cancelled for various reasons:

- User sends `CLUSTER CANCELMIGRATION ALL`
- Slot ownership changes during migration
- Source node is demoted to replica
- `FLUSHDB` is executed
- Connection is lost between source and target
- Authentication fails
- Snapshot process fails (e.g., child process OOM)
- No acknowledgment from target (timeout)
- Client output buffer overruns

## Features

### Asynchronous Operation

The `CLUSTER MIGRATESLOTS` command returns immediately after starting the migration. Use `CLUSTER GETSLOTMIGRATIONS` to monitor progress.

### Replication Support

Since the snapshot uses AOF format, the snapshot can be replayed verbatim to any replicas of the target node. The implementation uses the same proxying mechanism as chaining replication to copy content sent by the source node directly to replica nodes.

### Multiple Simultaneous Migrations

You can migrate multiple slot ranges to different nodes simultaneously by specifying multiple `SLOTSRANGE ... NODE ...` pairs in a single command.

## Version

These commands are available in Valkey 9.0 and later.

## References

- [Valkey PR #1949](https://github.com/valkey-io/valkey/pull/1949) - Atomic slot migration implementation
- [Valkey Issue #23](https://github.com/valkey-io/valkey/issues/23) - Original design discussion

