package valkeyotel

import (
	"context"
	"strings"

	"github.com/valkey-io/valkey-go"
)

type OpNameResolver interface {
	OpName(ctx context.Context, cmd valkey.Completed) string
	MultiOpName(ctx context.Context, cmds valkey.Commands) string
	MultiCacheableOpName(ctx context.Context, cmds []valkey.CacheableTTL) string
}

var _ OpNameResolver = (*DefaultOpNameResolver)(nil)

type DefaultOpNameResolver struct {
	// Limit controls how many elements are used to compose the resulting operation name.
	// If Limit is greater than zero, only the first Limit commands are used.
	Limit int
}

func (DefaultOpNameResolver) OpName(_ context.Context, cmd valkey.Completed) string {
	return cmd.Commands()[0]
}

func (r DefaultOpNameResolver) MultiOpName(_ context.Context, cmds valkey.Commands) string {
	switch len(cmds) {
	case 0:
		return ""
	case 1:
		return cmds[0].Commands()[0]
	}
	if r.Limit > 0 && len(cmds) > r.Limit {
		cmds = cmds[:r.Limit]
	}

	size := len(cmds) - 1
	for _, cmd := range cmds {
		size += len(cmd.Commands()[0])
	}

	sb := new(strings.Builder)
	sb.Grow(size)
	sb.WriteString(cmds[0].Commands()[0])
	for _, cmd := range cmds[1:] {
		sb.WriteRune(' ')
		sb.WriteString(cmd.Commands()[0])
	}
	return sb.String()
}

func (r DefaultOpNameResolver) MultiCacheableOpName(_ context.Context, cmds []valkey.CacheableTTL) string {
	switch len(cmds) {
	case 0:
		return ""
	case 1:
		return cmds[0].Cmd.Commands()[0]
	}
	if r.Limit > 0 && len(cmds) > r.Limit {
		cmds = cmds[:r.Limit]
	}

	size := len(cmds) - 1
	for _, cmd := range cmds {
		size += len(cmd.Cmd.Commands()[0])
	}

	sb := new(strings.Builder)
	sb.Grow(size)
	sb.WriteString(cmds[0].Cmd.Commands()[0])
	for _, cmd := range cmds[1:] {
		sb.WriteRune(' ')
		sb.WriteString(cmd.Cmd.Commands()[0])
	}
	return sb.String()
}
