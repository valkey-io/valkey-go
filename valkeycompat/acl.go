package valkeycompat

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/valkey-io/valkey-go"
)

// parseClientInfo parses raw client connection info strings into *ClientInfo.
func parseClientInfo(txt string) (*ClientInfo, error) {
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

// parseACLLog decodes nested RESP3 maps or RESP2 flat key-value arrays into []*ACLLogEntry.
func parseACLLog(res valkey.ValkeyResult) ([]*ACLLogEntry, error) {
	msg, err := res.ToMessage()
	if err != nil {
		return nil, err
	}
	return parseACLLogMessage(msg)
}

// parseACLLogMessage decodes an array ValkeyMessage containing ACL log entries.
func parseACLLogMessage(msg valkey.ValkeyMessage) ([]*ACLLogEntry, error) {
	arr, err := msg.ToArray()
	if err != nil {
		return nil, err
	}
	logEntries := make([]*ACLLogEntry, 0, len(arr))
	for _, entryMsg := range arr {
		log, err := entryMsg.AsMap()
		if err != nil {
			return nil, err
		}
		entry := &ACLLogEntry{}
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
					entry.ClientInfo, err = parseClientInfo(txt)
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
