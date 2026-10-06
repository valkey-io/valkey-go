package valkeycompat

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/valkey-io/valkey-go"
)

// ClientFlags represents valkey-server client flags
type ClientFlags uint64

const (
	ClientSlave               ClientFlags = 1 << 0  /* This client is a replica */
	ClientMaster              ClientFlags = 1 << 1  /* This client is a master */
	ClientMonitor             ClientFlags = 1 << 2  /* This client is a slave monitor, see MONITOR */
	ClientMulti               ClientFlags = 1 << 3  /* This client is in a MULTI context */
	ClientBlocked             ClientFlags = 1 << 4  /* The client is waiting in a blocking operation */
	ClientDirtyCAS            ClientFlags = 1 << 5  /* Watched keys modified. EXEC will fail. */
	ClientCloseAfterReply     ClientFlags = 1 << 6  /* Close after writing the entire reply. */
	ClientUnBlocked           ClientFlags = 1 << 7  /* This client was unblocked and is stored in server.unblocked_clients */
	ClientScript              ClientFlags = 1 << 8  /* This is a non-connected client used by Lua */
	ClientAsking              ClientFlags = 1 << 9  /* Client issued the ASKING command */
	ClientCloseASAP           ClientFlags = 1 << 10 /* Close this client ASAP */
	ClientUnixSocket          ClientFlags = 1 << 11 /* Client connected via Unix domain socket */
	ClientDirtyExec           ClientFlags = 1 << 12 /* EXEC will fail for errors while queueing */
	ClientMasterForceReply    ClientFlags = 1 << 13 /* Queue replies even if is master */
	ClientForceAOF            ClientFlags = 1 << 14 /* Force AOF propagation of current cmd. */
	ClientForceRepl           ClientFlags = 1 << 15 /* Force replication of the current cmd. */
	ClientPrePSync            ClientFlags = 1 << 16 /* Instance don't understand PSYNC. */
	ClientReadOnly            ClientFlags = 1 << 17 /* Cluster client is in the read-only state. */
	ClientPubSub              ClientFlags = 1 << 18 /* Client is in Pub/Sub mode. */
	ClientPreventAOFProp      ClientFlags = 1 << 19 /* Don't propagate to AOF. */
	ClientPreventReplProp     ClientFlags = 1 << 20 /* Don't propagate to slaves. */
	ClientPreventProp         ClientFlags = ClientPreventAOFProp | ClientPreventReplProp
	ClientPendingWrite        ClientFlags = 1 << 21 /* Client has output to send, but a-write handler is yet not installed. */
	ClientReplyOff            ClientFlags = 1 << 22 /* Don't send replies to a client. */
	ClientReplySkipNext       ClientFlags = 1 << 23 /* Set ClientREPLY_SKIP for the next cmd */
	ClientReplySkip           ClientFlags = 1 << 24 /* Don't send just this reply. */
	ClientLuaDebug            ClientFlags = 1 << 25 /* Run EVAL in debug mode. */
	ClientLuaDebugSync        ClientFlags = 1 << 26 /* EVAL debugging without fork() */
	ClientModule              ClientFlags = 1 << 27 /* Non-connected client used by some module. */
	ClientProtected           ClientFlags = 1 << 28 /* Client should not be freed for now. */
	ClientExecutingCommand    ClientFlags = 1 << 29 /* Indicates that the client is currently in the process of handling a command. */
	ClientPendingCommand      ClientFlags = 1 << 30 /* Indicates the client has a fully parsed command ready for execution. */
	ClientTracking            ClientFlags = 1 << 31 /* Client enabled key tracking in order to perform client side caching. */
	ClientTrackingBrokenRedir ClientFlags = 1 << 32 /* Target client is invalid. */
	ClientTrackingBCAST       ClientFlags = 1 << 33 /* Tracking in BCAST mode. */
	ClientTrackingOptIn       ClientFlags = 1 << 34 /* Tracking in opt-in mode. */
	ClientTrackingOptOut      ClientFlags = 1 << 35 /* Tracking in opt-out mode. */
	ClientTrackingCaching     ClientFlags = 1 << 36 /* CACHING yes/no was given, depending on opt-in/opt-out mode. */
	ClientTrackingNoLoop      ClientFlags = 1 << 37 /* Don't send invalidation messages about writes performed by myself. */
	ClientInTimeoutTable      ClientFlags = 1 << 38 /* This client is in the timeout table. */
	ClientProtocolError       ClientFlags = 1 << 39 /* Protocol error chatting with it. */
	ClientCloseAfterCommand   ClientFlags = 1 << 40 /* Close after executing commands and writing the entire reply. */
	ClientDenyBlocking        ClientFlags = 1 << 41 /* Indicate that the client should not be blocked. */
	ClientReplRDBOnly         ClientFlags = 1 << 42 /* This client is a replica that only wants RDB without a replication buffer. */
	ClientNoEvict             ClientFlags = 1 << 43 /* This client is protected against client memory eviction. */
	ClientAllowOOM            ClientFlags = 1 << 44 /* Client used by RM_Call is allowed to fully execute scripts even when in OOM. */
	ClientNoTouch             ClientFlags = 1 << 45 /* This client will not touch LFU/LRU stats. */
	ClientPushing             ClientFlags = 1 << 46 /* This client is pushing notifications. */
)

// ClientInfo represents parsed client connection telemetry from CLIENT INFO or ACL LOG
type ClientInfo struct {
	Addr               string        // address/port of the client
	LAddr              string        // address/port of a local address client connected to (bind address)
	Name               string        // the name set by the client with CLIENT SETNAME
	Events             string        // file descriptor events
	LastCmd            string        // last command executed
	User               string        // the authenticated username of the client
	LibName            string        // client library name
	LibVer             string        // client library version
	ID                 int64         // unique 64-bit client ID
	FD                 int64         // file descriptor corresponding to the socket
	Age                time.Duration // total duration of the connection
	Idle               time.Duration // idle time of the connection
	Flags              ClientFlags   // client flags
	DB                 int           // current database ID
	Sub                int           // number of channel subscriptions
	PSub               int           // number of pattern matching subscriptions
	SSub               int           // number of shard channel subscriptions
	Multi              int           // number of commands in a MULTI/EXEC context
	Watch              int           // number of keys this client is currently watching
	QueryBuf           int           // query buffer length (0 means no query pending)
	QueryBufFree       int           // free space of the query buffer (0 means buffer is full)
	ArgvMem            int           // incomplete arguments for the next command
	MultiMem           int           // memory used up by buffered multi commands
	BufferSize         int           // usable size of buffer
	BufferPeak         int           // peak used size of buffer in the last 5 sec interval
	OutputBufferLength int           // output buffer length
	OutputListLength   int           // output list length
	OutputMemory       int           // output buffer memory usage
	TotalMemory        int           // total memory consumed by this client
	Redir              int64         // client id of current client tracking redirection
	Resp               int           // client RESP protocol version
	TotalNetIn         int64         // total network bytes read from client
	TotalNetOut        int64         // total network bytes sent to client
	TotalCmds          int64         // number of commands executed by client
}

// ACLLogEntry represents a security log record from ACL LOG
type ACLLogEntry struct {
	Count                int64
	Reason               string
	Context              string
	Object               string
	Username             string
	AgeSeconds           float64
	ClientInfo           *ClientInfo
	EntryID              int64
	TimestampCreated     int64
	TimestampLastUpdated int64
}

// ACLDeniedReason indicates the categorized failure reason of an ACL DRYRUN execution
type ACLDeniedReason int

const (
	DeniedNone ACLDeniedReason = iota
	DeniedCommand
	DeniedKey
	DeniedChannel
	DeniedDatabase
	DeniedOther
)

// String returns the string representation of an ACLDeniedReason
func (r ACLDeniedReason) String() string {
	switch r {
	case DeniedNone:
		return "none"
	case DeniedCommand:
		return "command"
	case DeniedKey:
		return "key"
	case DeniedChannel:
		return "channel"
	case DeniedDatabase:
		return "database"
	default:
		return "other"
	}
}

// ACLDryRunResult contains the structured result of an ACL DRYRUN simulation
type ACLDryRunResult struct {
	Allowed    bool
	Reason     string
	DeniedType ACLDeniedReason
}

// ParseClientInfo parses raw client connection info strings into *ClientInfo.
func ParseClientInfo(txt string) (*ClientInfo, error) {
	info := &ClientInfo{}
	var err error
	txt = strings.TrimPrefix(strings.TrimSpace(txt), "txt:")
	fields := strings.Fields(txt)
	for _, s := range fields {
		key, val, ok := strings.Cut(s, "=")
		if !ok {
			return nil, fmt.Errorf("valkey: unexpected client info data (%s)", s)
		}

		switch key {
		case "id":
			info.ID, err = strconv.ParseInt(val, 10, 64)
		case "addr":
			info.Addr = val
		case "laddr":
			info.LAddr = val
		case "fd":
			info.FD, err = strconv.ParseInt(val, 10, 64)
		case "name":
			info.Name = val
		case "age":
			var age int
			if age, err = strconv.Atoi(val); err == nil {
				info.Age = time.Duration(age) * time.Second
			}
		case "idle":
			var idle int
			if idle, err = strconv.Atoi(val); err == nil {
				info.Idle = time.Duration(idle) * time.Second
			}
		case "flags":
			if val == "N" {
				break
			}
			for i := 0; i < len(val); i++ {
				switch val[i] {
				case 'S':
					info.Flags |= ClientSlave
				case 'O':
					info.Flags |= ClientSlave | ClientMonitor
				case 'M':
					info.Flags |= ClientMaster
				case 'P':
					info.Flags |= ClientPubSub
				case 'x':
					info.Flags |= ClientMulti
				case 'b':
					info.Flags |= ClientBlocked
				case 't':
					info.Flags |= ClientTracking
				case 'R':
					info.Flags |= ClientTrackingBrokenRedir
				case 'B':
					info.Flags |= ClientTrackingBCAST
				case 'd':
					info.Flags |= ClientDirtyCAS
				case 'c':
					info.Flags |= ClientCloseAfterCommand
				case 'u':
					info.Flags |= ClientUnBlocked
				case 'A':
					info.Flags |= ClientCloseASAP
				case 'U':
					info.Flags |= ClientUnixSocket
				case 'r':
					info.Flags |= ClientReadOnly
				case 'e':
					info.Flags |= ClientNoEvict
				case 'T':
					info.Flags |= ClientNoTouch
				default:
					// Forward compatibility: servers can return client-flag characters this client
					// does not recognize (new flags are added over time). Skip them instead of failing,
					// matching the skip-unknown-fields behavior of client list/info.
				}
			}
		case "db":
			info.DB, err = strconv.Atoi(val)
		case "sub":
			info.Sub, err = strconv.Atoi(val)
		case "psub":
			info.PSub, err = strconv.Atoi(val)
		case "ssub":
			info.SSub, err = strconv.Atoi(val)
		case "multi":
			info.Multi, err = strconv.Atoi(val)
		case "watch":
			info.Watch, err = strconv.Atoi(val)
		case "qbuf":
			info.QueryBuf, err = strconv.Atoi(val)
		case "qbuf-free":
			info.QueryBufFree, err = strconv.Atoi(val)
		case "argv-mem":
			info.ArgvMem, err = strconv.Atoi(val)
		case "multi-mem":
			info.MultiMem, err = strconv.Atoi(val)
		case "rbs":
			info.BufferSize, err = strconv.Atoi(val)
		case "rbp":
			info.BufferPeak, err = strconv.Atoi(val)
		case "obl":
			info.OutputBufferLength, err = strconv.Atoi(val)
		case "oll":
			info.OutputListLength, err = strconv.Atoi(val)
		case "omem":
			info.OutputMemory, err = strconv.Atoi(val)
		case "tot-mem":
			info.TotalMemory, err = strconv.Atoi(val)
		case "events":
			info.Events = val
		case "cmd":
			info.LastCmd = val
		case "user":
			info.User = val
		case "redir":
			info.Redir, err = strconv.ParseInt(val, 10, 64)
		case "resp":
			info.Resp, err = strconv.Atoi(val)
		case "lib-name":
			info.LibName = val
		case "lib-ver":
			info.LibVer = val
		case "tot-net-in":
			info.TotalNetIn, err = strconv.ParseInt(val, 10, 64)
		case "tot-net-out":
			info.TotalNetOut, err = strconv.ParseInt(val, 10, 64)
		case "tot-cmds":
			info.TotalCmds, err = strconv.ParseInt(val, 10, 64)
		}

		if err != nil {
			return nil, err
		}
	}
	return info, nil
}


// parseACLLog decodes nested RESP3 maps or RESP2 flat key-value arrays into []ACLLogEntry.
func parseACLLog(res valkey.ValkeyResult) ([]ACLLogEntry, error) {
	msg, err := res.ToMessage()
	if err != nil {
		return nil, err
	}
	return parseACLLogMessage(msg)
}

// parseACLLogMessage decodes an array ValkeyMessage containing ACL log entries.
func parseACLLogMessage(msg valkey.ValkeyMessage) ([]ACLLogEntry, error) {
	arr, err := msg.ToArray()
	if err != nil {
		return nil, err
	}
	logEntries := make([]ACLLogEntry, 0, len(arr))
	for _, entryMsg := range arr {
		log, err := entryMsg.AsMap()
		if err != nil {
			return nil, err
		}
		entry := ACLLogEntry{}
		for key, attr := range log {
			switch key {
			case "count":
				entry.Count, err = attr.AsInt64()
			case "reason":
				entry.Reason, err = attr.ToString()
			case "context":
				entry.Context, err = attr.ToString()
			case "object":
				entry.Object, err = attr.ToString()
			case "username":
				entry.Username, err = attr.ToString()
			case "age-seconds":
				entry.AgeSeconds, err = attr.AsFloat64()
			case "client-info":
				txt, txtErr := attr.ToString()
				if txtErr == nil && txt != "" {
					entry.ClientInfo, err = ParseClientInfo(txt)
				} else {
					err = txtErr
				}
			case "entry-id":
				entry.EntryID, err = attr.AsInt64()
			case "timestamp-created":
				entry.TimestampCreated, err = attr.AsInt64()
			case "timestamp-last-updated":
				entry.TimestampLastUpdated, err = attr.AsInt64()
			}
			if err != nil {
				return nil, err
			}
		}
		logEntries = append(logEntries, entry)
	}
	return logEntries, nil
}

// parseACLDryRun converts a raw ValkeyResult from ACL DRYRUN into an ACLDryRunResult.
func parseACLDryRun(res valkey.ValkeyResult) (ACLDryRunResult, error) {
	if err := res.Error(); err != nil {
		return ACLDryRunResult{}, err
	}
	str, err := res.ToString()
	if err != nil {
		return ACLDryRunResult{}, err
	}
	return parseACLDryRunString(str), nil
}

// parseACLDryRunString parses a dry-run result string and categorizes authorization status.
func parseACLDryRunString(s string) ACLDryRunResult {
	if s == "OK" {
		return ACLDryRunResult{
			Allowed:    true,
			Reason:     "OK",
			DeniedType: DeniedNone,
		}
	}

	lower := strings.ToLower(s)
	var deniedType ACLDeniedReason
	switch {
	case strings.HasSuffix(lower, " key") || strings.Contains(lower, "access a key"):
		deniedType = DeniedKey
	case strings.HasSuffix(lower, " channel") || strings.Contains(lower, "access a channel"):
		deniedType = DeniedChannel
	case strings.Contains(lower, "database"):
		deniedType = DeniedDatabase
	case strings.HasSuffix(lower, " command") || strings.Contains(lower, "to run ") || strings.Contains(lower, "access a command") || strings.Contains(lower, "command"):
		deniedType = DeniedCommand
	case strings.Contains(lower, "key"):
		deniedType = DeniedKey
	case strings.Contains(lower, "channel"):
		deniedType = DeniedChannel
	default:
		deniedType = DeniedOther
	}

	return ACLDryRunResult{
		Allowed:    false,
		Reason:     s,
		DeniedType: deniedType,
	}
}

