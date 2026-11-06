package gcp

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
)

// partsExporter records the collections it is asked to send, and fails the calls listed in
// fail. Only Export is called on it.
type partsExporter struct {
	sdkmetric.Exporter
	parts []*metricdata.ResourceMetrics
	fail  map[int]error // by call, from 0
}

func (e *partsExporter) Export(_ context.Context, rm *metricdata.ResourceMetrics) error {
	e.parts = append(e.parts, rm)
	return e.fail[len(e.parts)-1]
}

// sixPoints returns a collection of 6 data points: scope a has a sum of 4 points (values 1 to
// 4) and a gauge without points; scope b has a histogram of 2 points.
func sixPoints() *metricdata.ResourceMetrics {
	sum := make([]metricdata.DataPoint[int64], 4)
	for i := range sum {
		sum[i].Value = int64(i + 1)
	}
	return &metricdata.ResourceMetrics{
		Resource: resource.NewSchemaless(attribute.String("service.name", "checkout")),
		ScopeMetrics: []metricdata.ScopeMetrics{
			{Scope: instrumentation.Scope{Name: "a"}, Metrics: []metricdata.Metrics{
				{Name: "sum", Data: metricdata.Sum[int64]{DataPoints: sum, Temporality: metricdata.CumulativeTemporality, IsMonotonic: true}},
				{Name: "empty", Data: metricdata.Gauge[float64]{}},
			}},
			{Scope: instrumentation.Scope{Name: "b"}, Metrics: []metricdata.Metrics{
				{Name: "hist", Data: metricdata.Histogram[float64]{DataPoints: make([]metricdata.HistogramDataPoint[float64], 2)}},
			}},
		},
	}
}

// layout describes a collection as scope/metric:points entries.
func layout(rm *metricdata.ResourceMetrics) []string {
	var out []string
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out = append(out, fmt.Sprintf("%s/%s:%d", sm.Scope.Name, m.Name, dataPointCount(m.Data)))
		}
	}
	return out
}

func TestSplitExporter(t *testing.T) {
	ctx := context.Background()

	t.Run("a collection within the limit goes as it is", func(t *testing.T) {
		e := &partsExporter{}
		rm := sixPoints()
		if err := (splitExporter{e, 6}).Export(ctx, rm); err != nil {
			t.Fatal(err)
		}
		if len(e.parts) != 1 || e.parts[0] != rm {
			t.Fatalf("got %d parts, want rm itself", len(e.parts))
		}
	})

	t.Run("a larger collection is split, in order", func(t *testing.T) {
		e := &partsExporter{}
		rm := sixPoints()
		if err := (splitExporter{e, 3}).Export(ctx, rm); err != nil {
			t.Fatal(err)
		}
		want := [][]string{
			{"a/sum:3"},
			{"a/sum:1", "a/empty:0", "b/hist:2"},
		}
		if len(e.parts) != len(want) {
			t.Fatalf("got %d parts, want %d", len(e.parts), len(want))
		}
		var values []int64
		for i, part := range e.parts {
			if got := layout(part); !slices.Equal(got, want[i]) {
				t.Errorf("part %d = %v, want %v", i, got, want[i])
			}
			if part.Resource != rm.Resource {
				t.Errorf("part %d lost the resource", i)
			}
			sum := part.ScopeMetrics[0].Metrics[0].Data.(metricdata.Sum[int64])
			if !sum.IsMonotonic || sum.Temporality != metricdata.CumulativeTemporality {
				t.Errorf("part %d: the sum lost its temporality or monotonicity: %+v", i, sum)
			}
			for _, p := range sum.DataPoints {
				values = append(values, p.Value)
			}
		}
		if !slices.Equal(values, []int64{1, 2, 3, 4}) {
			t.Errorf("sum values across parts = %v, want 1 to 4", values)
		}
		if got := layout(rm); !slices.Equal(got, []string{"a/sum:4", "a/empty:0", "b/hist:2"}) {
			t.Errorf("the original collection changed: %v", got)
		}
	})

	t.Run("every part is sent when one fails", func(t *testing.T) {
		e := &partsExporter{fail: map[int]error{0: errors.New("rejected")}}
		err := (splitExporter{e, 3}).Export(ctx, sixPoints())
		if err == nil || err.Error() != "rejected" {
			t.Errorf("got %v, want the error of the failed part", err)
		}
		if len(e.parts) != 2 {
			t.Errorf("%d parts sent, want 2", len(e.parts))
		}
	})

	t.Run("no limit", func(t *testing.T) {
		e := &partsExporter{}
		if err := (splitExporter{e, 0}).Export(ctx, sixPoints()); err != nil || len(e.parts) != 1 {
			t.Errorf("got %d parts, %v; want the collection whole", len(e.parts), err)
		}
	})
}

// otherAggregation is an aggregation type that the split doesn't know.
type otherAggregation struct{ metricdata.Aggregation }

func TestDataPoints(t *testing.T) {
	for _, tc := range []struct {
		data metricdata.Aggregation
		want int
	}{
		{metricdata.Gauge[int64]{DataPoints: make([]metricdata.DataPoint[int64], 2)}, 2},
		{metricdata.Gauge[float64]{DataPoints: make([]metricdata.DataPoint[float64], 3)}, 3},
		{metricdata.Sum[int64]{DataPoints: make([]metricdata.DataPoint[int64], 4)}, 4},
		{metricdata.Sum[float64]{DataPoints: make([]metricdata.DataPoint[float64], 5)}, 5},
		{metricdata.Histogram[int64]{DataPoints: make([]metricdata.HistogramDataPoint[int64], 6)}, 6},
		{metricdata.Histogram[float64]{DataPoints: make([]metricdata.HistogramDataPoint[float64], 7)}, 7},
		{metricdata.ExponentialHistogram[int64]{DataPoints: make([]metricdata.ExponentialHistogramDataPoint[int64], 8)}, 8},
		{metricdata.ExponentialHistogram[float64]{DataPoints: make([]metricdata.ExponentialHistogramDataPoint[float64], 9)}, 9},
		{metricdata.Summary{DataPoints: make([]metricdata.SummaryDataPoint, 10)}, 10},
	} {
		if got := dataPointCount(tc.data); got != tc.want {
			t.Errorf("%T: %d points, want %d", tc.data, got, tc.want)
		}
		sliced := sliceDataPoints(tc.data, 1, 2)
		if got := dataPointCount(sliced); got != 1 {
			t.Errorf("%T sliced to [1, 2): %d points, want 1", tc.data, got)
		}
		if fmt.Sprintf("%T", sliced) != fmt.Sprintf("%T", tc.data) {
			t.Errorf("%T sliced to %T", tc.data, sliced)
		}
	}
	if got := dataPointCount(nil); got != 0 {
		t.Errorf("no data: %d points, want 0", got)
	}
	other := otherAggregation{}
	if got := dataPointCount(other); got != 1 {
		t.Errorf("unknown aggregation: %d points, want 1", got)
	}
	if got := sliceDataPoints(other, 0, 1); got != other {
		t.Errorf("an unknown aggregation was changed: %v", got)
	}
}
