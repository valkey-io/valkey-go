package valkey

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
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

// ACLSelector represents an isolated selector block (...) in ACL rules
type ACLSelector struct {
	Commands     string
	Keys         []string
	Channels     []string
	Databases    []int
	AllDatabases bool
	Raw          string
}

// ACLUser represents a parsed user definition from ACL LIST
type ACLUser struct {
	Username     string
	Enabled      bool
	NoPass       bool
	Passwords    []string
	Flags        []string
	Commands     string
	Keys         []string
	Channels     []string
	Databases    []int
	AllDatabases bool
	Selectors    []ACLSelector
	Raw          string
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

func tokenizeACL(raw string) []string {
	var tokens []string
	raw = strings.TrimSpace(raw)
	n := len(raw)
	i := 0
	for i < n {
		for i < n && (raw[i] == ' ' || raw[i] == '\t' || raw[i] == '\r' || raw[i] == '\n') {
			i++
		}
		if i >= n {
			break
		}
		if raw[i] == '(' {
			start := i
			depth := 1
			i++
			for i < n && depth > 0 {
				if raw[i] == '(' {
					depth++
				} else if raw[i] == ')' {
					depth--
				}
				i++
			}
			tokens = append(tokens, raw[start:i])
		} else {
			start := i
			for i < n && !(raw[i] == ' ' || raw[i] == '\t' || raw[i] == '\r' || raw[i] == '\n' || raw[i] == '(') {
				i++
			}
			tokens = append(tokens, raw[start:i])
		}
	}
	return tokens
}

func parseSelector(raw string) ACLSelector {
	sel := ACLSelector{
		Raw:          raw,
		AllDatabases: true,
	}
	inner := strings.TrimSpace(raw)
	if strings.HasPrefix(inner, "(") && strings.HasSuffix(inner, ")") {
		inner = strings.TrimSpace(inner[1 : len(inner)-1])
	}
	tokens := strings.Fields(inner)
	for _, tok := range tokens {
		switch {
		case tok == "allkeys":
			sel.Keys = []string{"~*"}
		case tok == "resetkeys":
			sel.Keys = nil
		case strings.HasPrefix(tok, "~") || strings.HasPrefix(tok, "%"):
			sel.Keys = append(sel.Keys, tok)
		case tok == "allchannels":
			sel.Channels = []string{"&*"}
		case tok == "resetchannels":
			sel.Channels = nil
		case strings.HasPrefix(tok, "&"):
			sel.Channels = append(sel.Channels, tok)
		case tok == "alldbs":
			sel.AllDatabases = true
			sel.Databases = nil
		case tok == "resetdbs":
			sel.AllDatabases = false
			sel.Databases = nil
		case strings.HasPrefix(tok, "db=") || strings.HasPrefix(tok, "db:"):
			sel.AllDatabases = false
			val := strings.TrimPrefix(strings.TrimPrefix(tok, "db="), "db:")
			for _, part := range strings.Split(val, ",") {
				if db, err := strconv.Atoi(part); err == nil {
					sel.Databases = append(sel.Databases, db)
				}
			}
		case strings.HasPrefix(tok, "+") || strings.HasPrefix(tok, "-") || tok == "allcommands" || tok == "nocommands":
			if sel.Commands == "" {
				sel.Commands = tok
			} else {
				sel.Commands += " " + tok
			}
		case tok == "reset":
			sel.Keys = nil
			sel.Channels = nil
			sel.Databases = nil
			sel.AllDatabases = true
			sel.Commands = "-@all"
		}
	}
	return sel
}

// ParseACLUser tokenizes and decodes an individual ACL rule DSL string into an ACLUser struct.
func ParseACLUser(raw string) (ACLUser, error) {
	tokens := tokenizeACL(raw)
	if len(tokens) == 0 {
		return ACLUser{}, errors.New("valkey: empty ACL rule")
	}

	user := ACLUser{
		Raw:          raw,
		AllDatabases: true,
	}

	startIdx := 0
	if tokens[0] == "user" {
		if len(tokens) < 2 {
			return ACLUser{}, errors.New("valkey: missing username after 'user'")
		}
		user.Username = tokens[1]
		startIdx = 2
	}

	for i := startIdx; i < len(tokens); i++ {
		tok := tokens[i]
		switch {
		case tok == "on":
			user.Enabled = true
		case tok == "off":
			user.Enabled = false
		case tok == "nopass":
			user.NoPass = true
			user.Passwords = nil
		case tok == "resetpass":
			user.NoPass = false
			user.Passwords = nil
		case strings.HasPrefix(tok, ">") || strings.HasPrefix(tok, "#"):
			user.Passwords = append(user.Passwords, tok)
		case strings.HasPrefix(tok, "<") || strings.HasPrefix(tok, "!"):
			// password removal directive
		case tok == "allkeys":
			user.Keys = []string{"~*"}
		case tok == "resetkeys":
			user.Keys = nil
		case strings.HasPrefix(tok, "~") || strings.HasPrefix(tok, "%"):
			user.Keys = append(user.Keys, tok)
		case tok == "allchannels":
			user.Channels = []string{"&*"}
		case tok == "resetchannels":
			user.Channels = nil
		case strings.HasPrefix(tok, "&"):
			user.Channels = append(user.Channels, tok)
		case tok == "alldbs":
			user.AllDatabases = true
			user.Databases = nil
		case tok == "resetdbs":
			user.AllDatabases = false
			user.Databases = nil
		case strings.HasPrefix(tok, "db=") || strings.HasPrefix(tok, "db:"):
			user.AllDatabases = false
			val := strings.TrimPrefix(strings.TrimPrefix(tok, "db="), "db:")
			for _, part := range strings.Split(val, ",") {
				if db, err := strconv.Atoi(part); err == nil {
					user.Databases = append(user.Databases, db)
				}
			}
		case strings.HasPrefix(tok, "+") || strings.HasPrefix(tok, "-") || tok == "allcommands" || tok == "nocommands":
			if user.Commands == "" {
				user.Commands = tok
			} else {
				user.Commands += " " + tok
			}
		case tok == "reset":
			user.Enabled = false
			user.NoPass = false
			user.Passwords = nil
			user.Keys = nil
			user.Channels = nil
			user.Databases = nil
			user.AllDatabases = true
			user.Commands = "-@all"
			user.Selectors = nil
		case tok == "clearselectors":
			user.Selectors = nil
		case strings.HasPrefix(tok, "(") && strings.HasSuffix(tok, ")"):
			user.Selectors = append(user.Selectors, parseSelector(tok))
		default:
			// Flags such as sanitize-payload, skip-sanitize-payload
			user.Flags = append(user.Flags, tok)
		}
	}
	return user, nil
}

// ParseACLListStrings decodes a slice of ACL rule DSL strings into []ACLUser.
func ParseACLListStrings(lines []string) ([]ACLUser, error) {
	users := make([]ACLUser, 0, len(lines))
	for _, line := range lines {
		user, err := ParseACLUser(line)
		if err != nil {
			return nil, err
		}
		users = append(users, user)
	}
	return users, nil
}

// ParseACLList decodes a multi-bulk array response from ACL LIST into []ACLUser.
func ParseACLList(res ValkeyResult) ([]ACLUser, error) {
	if err := res.Error(); err != nil {
		return nil, err
	}
	lines, err := res.AsStrSlice()
	if err != nil {
		return nil, err
	}
	return ParseACLListStrings(lines)
}

// ParseACLLog decodes nested RESP3 maps or RESP2 flat key-value arrays into []ACLLogEntry.
func ParseACLLog(res ValkeyResult) ([]ACLLogEntry, error) {
	if err := res.Error(); err != nil {
		return nil, err
	}
	return ParseACLLogMessage(res.val)
}

// ParseACLLogMessage decodes an array ValkeyMessage containing ACL log entries.
func ParseACLLogMessage(msg ValkeyMessage) ([]ACLLogEntry, error) {
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

// ParseACLDryRun converts a raw ValkeyResult from ACL DRYRUN into an ACLDryRunResult.
func ParseACLDryRun(res ValkeyResult) (ACLDryRunResult, error) {
	if err := res.Error(); err != nil {
		return ACLDryRunResult{}, err
	}
	str, err := res.ToString()
	if err != nil {
		return ACLDryRunResult{}, err
	}
	return ParseACLDryRunString(str), nil
}

// ParseACLDryRunString parses a dry-run result string and categorizes authorization status.
func ParseACLDryRunString(s string) ACLDryRunResult {
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

// ACLList executes ACL LIST and parses the result into []ACLUser.
func ACLList(ctx context.Context, client Client) ([]ACLUser, error) {
	cmd := client.B().AclList().Build()
	res := client.Do(ctx, cmd)
	return ParseACLList(res)
}

// ACLLog executes ACL LOG [count] and parses the entries into []ACLLogEntry.
func ACLLog(ctx context.Context, client Client, count int64) ([]ACLLogEntry, error) {
	var cmd Completed
	if count > 0 {
		cmd = client.B().AclLog().Count(count).Build()
	} else {
		cmd = client.B().Arbitrary("ACL", "LOG").Build()
	}
	res := client.Do(ctx, cmd)
	return ParseACLLog(res)
}

// ACLDryRun executes ACL DRYRUN <username> <command> [args...] and evaluates authorization.
func ACLDryRun(ctx context.Context, client Client, username string, command ...string) (ACLDryRunResult, error) {
	if len(command) == 0 {
		return ACLDryRunResult{}, errors.New("valkey: command is required for ACLDryRun")
	}
	var cmd Completed
	if len(command) == 1 {
		cmd = client.B().AclDryrun().Username(username).Command(command[0]).Build()
	} else {
		cmd = client.B().AclDryrun().Username(username).Command(command[0]).Arg(command[1:]...).Build()
	}
	res := client.Do(ctx, cmd)
	return ParseACLDryRun(res)
}

