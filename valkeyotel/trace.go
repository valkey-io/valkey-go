package valkeyotel

import (
	"context"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/valkey-io/valkey-go"
)

var (
	name   = "github.com/valkey-io/valkey-go"
	kind   = trace.WithSpanKind(trace.SpanKindClient)
	dbattr = attribute.String("db.system", "valkey")
	dbstmt = attribute.Key("db.statement")
)

var _ valkey.Client = (*otelclient)(nil)

// WithClient creates a new valkey.Client with OpenTelemetry tracing enabled.
//
// Deprecated: use NewClient() instead.
func WithClient(client valkey.Client, opts ...Option) valkey.Client {
	cli, err := newClient(opts...)
	if err != nil {
		panic(err)
	}
	cli.client = client
	return cli
}

// Option is the Functional Options interface
type Option func(o *otelclient)

// TraceAttrs set additional attributes to append to each trace.
func TraceAttrs(attrs ...attribute.KeyValue) Option {
	return func(o *otelclient) {
		o.tAttrs = trace.WithAttributes(attrs...)
	}
}

// WithTracerProvider sets the TracerProvider for the otelclient.
func WithTracerProvider(provider trace.TracerProvider) Option {
	return func(o *otelclient) {
		o.tracerProvider = provider
	}
}

// WithDBStatement tells the tracing hook to add raw valkey commands to db.statement attribute.
func WithDBStatement(f StatementFunc) Option {
	return func(o *otelclient) {
		o.dbStmtFunc = f
	}
}

// StatementFunc is a the function that maps a command's tokens to a string to put in the db.statement attribute
type StatementFunc func(cmdTokens []string) string

type otelclient struct {
	client          valkey.Client
	meterProvider   metric.MeterProvider
	tracerProvider  trace.TracerProvider
	tracer          trace.Tracer
	meter           metric.Meter
	cscMiss         metric.Int64Counter
	cscHits         metric.Int64Counter
	mAttrs          metric.MeasurementOption
	tAttrs          trace.SpanStartEventOption
	histogramOption HistogramOption
	dbStmtFunc      StatementFunc
}

func (o *otelclient) B() valkey.Builder {
	return o.client.B()
}

func (o *otelclient) Do(ctx context.Context, cmd valkey.Completed) (resp valkey.ValkeyResult) {
	ctx, span := o.start(ctx, first(cmd.Commands()), sum(cmd.Commands()), o.tAttrs)
	if o.dbStmtFunc != nil {
		span.SetAttributes(dbstmt.String(o.dbStmtFunc(cmd.Commands())))
	}

	resp = o.client.Do(ctx, cmd)
	o.end(span, resp.Error())
	return
}

func (o *otelclient) DoMulti(ctx context.Context, multi ...valkey.Completed) (resp []valkey.ValkeyResult) {
	ctx, span := o.start(ctx, multiFirst(multi), multiSum(multi), o.tAttrs)
	resp = o.client.DoMulti(ctx, multi...)
	o.end(span, firstError(resp))
	return
}

func (o *otelclient) DoStream(ctx context.Context, cmd valkey.Completed) (resp valkey.ValkeyResultStream) {
	ctx, span := o.start(ctx, first(cmd.Commands()), sum(cmd.Commands()), o.tAttrs)
	if o.dbStmtFunc != nil {
		span.SetAttributes(dbstmt.String(o.dbStmtFunc(cmd.Commands())))
	}

	resp = o.client.DoStream(ctx, cmd)
	o.end(span, resp.Error())
	return
}

func (o *otelclient) DoMultiStream(ctx context.Context, multi ...valkey.Completed) (resp valkey.MultiValkeyResultStream) {
	ctx, span := o.start(ctx, multiFirst(multi), multiSum(multi), o.tAttrs)
	resp = o.client.DoMultiStream(ctx, multi...)
	o.end(span, resp.Error())
	return
}

func (o *otelclient) DoCache(ctx context.Context, cmd valkey.Cacheable, ttl time.Duration) (resp valkey.ValkeyResult) {
	ctx, span := o.start(ctx, first(cmd.Commands()), sum(cmd.Commands()), o.tAttrs)
	if o.dbStmtFunc != nil {
		span.SetAttributes(dbstmt.String(o.dbStmtFunc(cmd.Commands())))
	}

	resp = o.client.DoCache(ctx, cmd, ttl)
	if resp.NonValkeyError() == nil {
		if resp.IsCacheHit() {
			o.cscHits.Add(ctx, 1, o.mAttrs)
		} else {
			o.cscMiss.Add(ctx, 1, o.mAttrs)
		}
	}
	o.end(span, resp.Error())
	return
}

func (o *otelclient) DoMultiCache(ctx context.Context, multi ...valkey.CacheableTTL) (resps []valkey.ValkeyResult) {
	ctx, span := o.start(ctx, multiCacheableFirst(multi), multiCacheableSum(multi), o.tAttrs)
	resps = o.client.DoMultiCache(ctx, multi...)
	for _, resp := range resps {
		if resp.NonValkeyError() == nil {
			if resp.IsCacheHit() {
				o.cscHits.Add(ctx, 1, o.mAttrs)
			} else {
				o.cscMiss.Add(ctx, 1, o.mAttrs)
			}
		}
	}
	o.end(span, firstError(resps))
	return
}

func (o *otelclient) Dedicated(fn func(valkey.DedicatedClient) error) (err error) {
	return o.client.Dedicated(func(client valkey.DedicatedClient) error {
		return fn(&dedicated{
			client:     client,
			tAttrs:     o.tAttrs,
			tracer:     o.tracer,
			dbStmtFunc: o.dbStmtFunc,
		})
	})
}

func (o *otelclient) Dedicate() (valkey.DedicatedClient, func()) {
	client, cancel := o.client.Dedicate()
	return &dedicated{
		client:     client,
		tAttrs:     o.tAttrs,
		tracer:     o.tracer,
		dbStmtFunc: o.dbStmtFunc,
	}, cancel
}

func (o *otelclient) Receive(ctx context.Context, subscribe valkey.Completed, fn func(msg valkey.PubSubMessage)) (err error) {
	ctx, span := o.start(ctx, first(subscribe.Commands()), sum(subscribe.Commands()), o.tAttrs)
	if o.dbStmtFunc != nil {
		span.SetAttributes(dbstmt.String(o.dbStmtFunc(subscribe.Commands())))
	}

	err = o.client.Receive(ctx, subscribe, fn)
	o.end(span, err)
	return
}

func (o *otelclient) Nodes() map[string]valkey.Client {
	nodes := o.client.Nodes()
	for addr, client := range nodes {
		nodes[addr] = &otelclient{
			client:          client,
			mAttrs:          o.mAttrs,
			tAttrs:          o.tAttrs,
			meterProvider:   o.meterProvider,
			tracerProvider:  o.tracerProvider,
			tracer:          o.tracer,
			meter:           o.meter,
			cscMiss:         o.cscMiss,
			cscHits:         o.cscHits,
			histogramOption: o.histogramOption,
			dbStmtFunc:      o.dbStmtFunc,
		}
	}
	return nodes
}

func (o *otelclient) Close() {
	o.client.Close()
}

var _ valkey.DedicatedClient = (*dedicated)(nil)

type dedicated struct {
	client     valkey.DedicatedClient
	tracer     trace.Tracer
	tAttrs     trace.SpanStartEventOption
	dbStmtFunc StatementFunc
}

func (d *dedicated) B() valkey.Builder {
	return d.client.B()
}

func (d *dedicated) Do(ctx context.Context, cmd valkey.Completed) (resp valkey.ValkeyResult) {
	ctx, span := d.start(ctx, first(cmd.Commands()), sum(cmd.Commands()), d.tAttrs)
	if d.dbStmtFunc != nil {
		span.SetAttributes(dbstmt.String(d.dbStmtFunc(cmd.Commands())))
	}

	resp = d.client.Do(ctx, cmd)
	d.end(span, resp.Error())
	return
}

func (d *dedicated) DoMulti(ctx context.Context, multi ...valkey.Completed) (resp []valkey.ValkeyResult) {
	ctx, span := d.start(ctx, multiFirst(multi), multiSum(multi), d.tAttrs)
	resp = d.client.DoMulti(ctx, multi...)
	d.end(span, firstError(resp))
	return
}

func (d *dedicated) Receive(ctx context.Context, subscribe valkey.Completed, fn func(msg valkey.PubSubMessage)) (err error) {
	ctx, span := d.start(ctx, first(subscribe.Commands()), sum(subscribe.Commands()), d.tAttrs)
	if d.dbStmtFunc != nil {
		span.SetAttributes(dbstmt.String(d.dbStmtFunc(subscribe.Commands())))
	}

	err = d.client.Receive(ctx, subscribe, fn)
	d.end(span, err)
	return
}

func (d *dedicated) SetPubSubHooks(hooks valkey.PubSubHooks) <-chan error {
	return d.client.SetPubSubHooks(hooks)
}

func (d *dedicated) Close() {
	d.client.Close()
}

func first(s []string) string {
	return s[0]
}

func sum(s []string) (v int) {
	for _, str := range s {
		v += len(str)
	}
	return v
}

func firstError(s []valkey.ValkeyResult) error {
	for _, result := range s {
		if err := result.Error(); err != nil && !valkey.IsValkeyNil(err) {
			return err
		}
	}
	return nil
}

func multiSum(multi []valkey.Completed) (v int) {
	for _, cmd := range multi {
		v += sum(cmd.Commands())
	}
	return v
}

func multiCacheableSum(multi []valkey.CacheableTTL) (v int) {
	for _, cmd := range multi {
		v += sum(cmd.Cmd.Commands())
	}
	return v
}

func multiFirst(multi []valkey.Completed) string {
	if len(multi) == 0 {
		return ""
	}

	if len(multi) > 5 {
		multi = multi[:5]
	}
	size := 0
	for _, cmd := range multi {
		size += len(first(cmd.Commands()))
	}
	size += len(multi) - 1

	sb := strings.Builder{}
	sb.Grow(size)
	for i, cmd := range multi {
		sb.WriteString(first(cmd.Commands()))
		if i != len(multi)-1 {
			sb.WriteString(" ")
		}
	}

	return sb.String()
}

func multiCacheableFirst(multi []valkey.CacheableTTL) string {
	if len(multi) == 0 {
		return ""
	}

	if len(multi) > 5 {
		multi = multi[:5]
	}
	size := 0
	for _, cmd := range multi {
		size += len(first(cmd.Cmd.Commands()))
	}
	size += len(multi) - 1

	sb := strings.Builder{}
	sb.Grow(size)
	for i, cmd := range multi {
		sb.WriteString(first(cmd.Cmd.Commands()))
		if i != len(multi)-1 {
			sb.WriteString(" ")
		}
	}
	return sb.String()
}

func (o *otelclient) start(ctx context.Context, op string, size int, attrs trace.SpanStartEventOption) (context.Context, trace.Span) {
	return startSpan(o.tracer, ctx, op, size, attrs)
}

func (o *otelclient) end(span trace.Span, err error) {
	endSpan(span, err)
}

func (d *dedicated) start(ctx context.Context, op string, size int, attrs trace.SpanStartEventOption) (context.Context, trace.Span) {
	return startSpan(d.tracer, ctx, op, size, attrs)
}

func (d *dedicated) end(span trace.Span, err error) {
	endSpan(span, err)
}

func startSpan(tracer trace.Tracer, ctx context.Context, op string, size int, attrs trace.SpanStartEventOption) (context.Context, trace.Span) {
	return tracer.Start(ctx, op, kind, attr(op, size), attrs)
}

func endSpan(span trace.Span, err error) {
	if err != nil && !valkey.IsValkeyNil(err) {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}
	span.End()
}

// do not record full db.statement to avoid collecting sensitive data
func attr(op string, size int) trace.SpanStartEventOption {
	return trace.WithAttributes(dbattr, attribute.String("db.operation", op), attribute.Int("db.stmt_size", size))
}
