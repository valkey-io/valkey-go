package gcp

import (
	"context"
	"errors"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// maxDataPointsPerRequest is the Telemetry API's limit on the data points in one metrics
// request. The OTLP exporter sends a whole collection in one request, so splitExporter splits
// larger ones. (OpenTelemetry Go has an experimental, opt-in equivalent,
// OTEL_GO_X_METRIC_EXPORT_BATCH_SIZE; splitExporter can give way to it once it is stable.)
const maxDataPointsPerRequest = 200

// splitExporter passes collections of at most limit data points to the exporter it wraps, and
// splits larger ones into several requests, so an application with many commands doesn't
// lose a whole interval of metrics to the Telemetry API's per-request limit.
type splitExporter struct {
	sdkmetric.Exporter
	limit int
}

// Export sends rm in requests of at most e.limit data points. It sends every part even when
// one fails, and returns the errors of all of them. A limit of 0 or less sends rm whole.
func (e splitExporter) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	if e.limit <= 0 || countDataPoints(rm) <= e.limit {
		return e.Exporter.Export(ctx, rm)
	}
	var errs []error
	for _, part := range splitResourceMetrics(rm, e.limit) {
		if err := e.Exporter.Export(ctx, part); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// countDataPoints returns the number of data points in rm.
func countDataPoints(rm *metricdata.ResourceMetrics) int {
	n := 0
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			n += dataPointCount(m.Data)
		}
	}
	return n
}

// splitResourceMetrics splits rm into parts of at most limit data points each, keeping the
// resource, the scopes and the order of metrics and points. A metric with more points than
// fit in the current part continues in the next one. limit must be positive.
func splitResourceMetrics(rm *metricdata.ResourceMetrics, limit int) []*metricdata.ResourceMetrics {
	var parts []*metricdata.ResourceMetrics
	cur := &metricdata.ResourceMetrics{Resource: rm.Resource}
	used := 0
	scopeOpen := false // whether cur's last ScopeMetrics belongs to the scope being split
	flush := func() {
		parts = append(parts, cur)
		cur = &metricdata.ResourceMetrics{Resource: rm.Resource}
		used, scopeOpen = 0, false
	}
	add := func(sm metricdata.ScopeMetrics, m metricdata.Metrics) {
		if !scopeOpen {
			cur.ScopeMetrics = append(cur.ScopeMetrics, metricdata.ScopeMetrics{Scope: sm.Scope})
			scopeOpen = true
		}
		last := &cur.ScopeMetrics[len(cur.ScopeMetrics)-1]
		last.Metrics = append(last.Metrics, m)
	}
	for _, sm := range rm.ScopeMetrics {
		scopeOpen = false
		for _, m := range sm.Metrics {
			total := dataPointCount(m.Data)
			if total == 0 {
				add(sm, m)
				continue
			}
			for start := 0; start < total; {
				if used == limit {
					flush()
				}
				end := min(start+limit-used, total)
				part := m
				part.Data = sliceDataPoints(m.Data, start, end)
				add(sm, part)
				used += end - start
				start = end
			}
		}
	}
	if used > 0 || len(cur.ScopeMetrics) > 0 {
		parts = append(parts, cur)
	}
	return parts
}

// dataPointCount returns the number of data points of an aggregation. An aggregation type it
// doesn't know counts as one point that is never split.
func dataPointCount(a metricdata.Aggregation) int {
	switch d := a.(type) {
	case metricdata.Gauge[int64]:
		return len(d.DataPoints)
	case metricdata.Gauge[float64]:
		return len(d.DataPoints)
	case metricdata.Sum[int64]:
		return len(d.DataPoints)
	case metricdata.Sum[float64]:
		return len(d.DataPoints)
	case metricdata.Histogram[int64]:
		return len(d.DataPoints)
	case metricdata.Histogram[float64]:
		return len(d.DataPoints)
	case metricdata.ExponentialHistogram[int64]:
		return len(d.DataPoints)
	case metricdata.ExponentialHistogram[float64]:
		return len(d.DataPoints)
	case metricdata.Summary:
		return len(d.DataPoints)
	case nil:
		return 0
	default:
		return 1
	}
}

// sliceDataPoints returns a copy of a with only its data points [i, j).
func sliceDataPoints(a metricdata.Aggregation, i, j int) metricdata.Aggregation {
	switch d := a.(type) {
	case metricdata.Gauge[int64]:
		d.DataPoints = d.DataPoints[i:j]
		return d
	case metricdata.Gauge[float64]:
		d.DataPoints = d.DataPoints[i:j]
		return d
	case metricdata.Sum[int64]:
		d.DataPoints = d.DataPoints[i:j]
		return d
	case metricdata.Sum[float64]:
		d.DataPoints = d.DataPoints[i:j]
		return d
	case metricdata.Histogram[int64]:
		d.DataPoints = d.DataPoints[i:j]
		return d
	case metricdata.Histogram[float64]:
		d.DataPoints = d.DataPoints[i:j]
		return d
	case metricdata.ExponentialHistogram[int64]:
		d.DataPoints = d.DataPoints[i:j]
		return d
	case metricdata.ExponentialHistogram[float64]:
		d.DataPoints = d.DataPoints[i:j]
		return d
	case metricdata.Summary:
		d.DataPoints = d.DataPoints[i:j]
		return d
	default:
		return a
	}
}
