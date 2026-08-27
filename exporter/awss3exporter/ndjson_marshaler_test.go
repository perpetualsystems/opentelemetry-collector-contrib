// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awss3exporter

import (
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// The three golden tests below pin the exact line format, which is the whole
// point of this marshaler: the output has to match what the equivalent jq
// pipeline produces for the same OTLP payload.

func TestNdjsonMarshalLogs(t *testing.T) {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("k8s.namespace.name", "simba")
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.Timestamp(1787617744022570766))
	lr.Body().SetStr("IngestJob completed successfully")
	lr.Attributes().PutStr("log.iostream", "stdout")

	out, err := ndjsonMarshaler{}.MarshalLogs(logs)
	require.NoError(t, err)
	assert.Equal(t,
		`{"timeUnixNano":"1787617744022570766","message":"IngestJob completed successfully",`+
			`"attributes":{"k8s.namespace.name":"simba","log.iostream":"stdout"}}`+"\n",
		string(out))
}

func TestNdjsonMarshalMetricsSumDataPoint(t *testing.T) {
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("k8s.cluster.name", "dev-use2-data")
	metric := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	metric.SetName("container_cpu_usage_seconds_total")
	metric.SetDescription("Cumulative cpu time consumed")
	dp := metric.SetEmptySum().DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.Timestamp(1787617817596000000))
	dp.SetIntValue(19)
	dp.Attributes().PutStr("container", "df-ingest")

	out, err := ndjsonMarshaler{}.MarshalMetrics(metrics)
	require.NoError(t, err)
	// No "unit" key: the metric does not set one.
	assert.Equal(t,
		`{"name":"container_cpu_usage_seconds_total","description":"Cumulative cpu time consumed",`+
			`"type":"sum","timeUnixNano":"1787617817596000000","asInt":19,`+
			`"attributes":{"container":"df-ingest","k8s.cluster.name":"dev-use2-data"}}`+"\n",
		string(out))
}

func TestNdjsonMarshalTraces(t *testing.T) {
	traces := ptrace.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "df-ingest")
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetTraceID(pcommon.TraceID([16]byte{0x5b, 0x8e, 0xff, 0xf7, 0x98, 0x03, 0x81, 0x03, 0xd2, 0x69, 0xb6, 0x33, 0x81, 0x3f, 0xc6, 0x0c}))
	span.SetSpanID(pcommon.SpanID([8]byte{0xee, 0xe1, 0x9b, 0x7e, 0xc3, 0xc1, 0xb1, 0x74}))
	span.SetParentSpanID(pcommon.SpanID([8]byte{0xee, 0xe1, 0x9b, 0x7e, 0xc3, 0xc1, 0xb1, 0x73}))
	span.SetName("HTTP GET /api/v1/tables")
	span.SetKind(ptrace.SpanKindServer)
	span.SetStartTimestamp(pcommon.Timestamp(1787617744022570766))
	span.SetEndTimestamp(pcommon.Timestamp(1787617744099120041))
	span.Status().SetCode(ptrace.StatusCodeOk)
	span.Attributes().PutInt("http.response.status_code", 200)

	out, err := ndjsonMarshaler{}.MarshalTraces(traces)
	require.NoError(t, err)
	assert.Equal(t,
		`{"traceId":"5b8efff798038103d269b633813fc60c","spanId":"eee19b7ec3c1b174",`+
			`"parentSpanId":"eee19b7ec3c1b173","name":"HTTP GET /api/v1/tables","kind":"server",`+
			`"startTimeUnixNano":"1787617744022570766","endTimeUnixNano":"1787617744099120041",`+
			`"status":{"code":"ok"},`+
			`"attributes":{"http.response.status_code":200,"service.name":"df-ingest"}}`+"\n",
		string(out))
}

func TestNdjsonMarshalerFormat(t *testing.T) {
	m, err := newMarshaler(NDJSON, zap.NewNop())
	require.NoError(t, err)
	require.NotNil(t, m)
	assert.Equal(t, "jsonl", m.format())
	assert.False(t, m.compressed())
}

// TestNdjsonMarshalerAllSignalsWired guards the nil dereference in s3Marshaler:
// a marshaler registered without all three pdata marshalers panics on the
// signal it left unset.
func TestNdjsonMarshalerAllSignalsWired(t *testing.T) {
	m, err := newMarshaler(NDJSON, zap.NewNop())
	require.NoError(t, err)

	require.NotPanics(t, func() {
		_, err = m.MarshalLogs(plog.NewLogs())
		assert.NoError(t, err)
		_, err = m.MarshalMetrics(pmetric.NewMetrics())
		assert.NoError(t, err)
		_, err = m.MarshalTraces(ptrace.NewTraces())
		assert.NoError(t, err)
	})
}

func TestNdjsonAttributePrecedence(t *testing.T) {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("shared", "resource")
	rl.Resource().Attributes().PutStr("from.resource", "yes")
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().Attributes().PutStr("shared", "scope")
	sl.Scope().Attributes().PutStr("from.scope", "yes")
	lr := sl.LogRecords().AppendEmpty()
	lr.Attributes().PutStr("shared", "record")

	attrs := unmarshalLine(t, marshalLogsToLine(t, logs))["attributes"].(map[string]any)
	assert.Equal(t, "record", attrs["shared"])
	assert.Equal(t, "yes", attrs["from.resource"])
	assert.Equal(t, "yes", attrs["from.scope"])
}

func TestNdjsonAttributeNativeTypes(t *testing.T) {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.Attributes().PutInt("int", 42)
	lr.Attributes().PutDouble("double", 1.5)
	lr.Attributes().PutBool("bool", true)
	lr.Attributes().PutEmptySlice("slice").AppendEmpty().SetInt(13)
	lr.Attributes().PutEmptyMap("map").PutInt("a", 1)

	line := marshalLogsToLine(t, logs)
	assert.Contains(t, line, `"int":42`)
	assert.Contains(t, line, `"double":1.5`)
	assert.Contains(t, line, `"bool":true`)
	assert.Contains(t, line, `"slice":[13]`)
	assert.Contains(t, line, `"map":{"a":1}`)
	// Values must not be stringified.
	assert.NotContains(t, line, `"int":"42"`)
}

func TestNdjsonLogStructuredBody(t *testing.T) {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	body := lr.Body().SetEmptyMap()
	body.PutStr("event", "ingest")
	body.PutInt("rows", 7)

	assert.Contains(t, marshalLogsToLine(t, logs), `"body":{"event":"ingest","rows":7}`)
}

func TestNdjsonLogTimestampFallsBackToObserved(t *testing.T) {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.SetObservedTimestamp(pcommon.Timestamp(1787617744174984128))
	lr.Body().SetStr("no timestamp parser configured")

	assert.Contains(t, marshalLogsToLine(t, logs), `"timeUnixNano":"1787617744174984128"`)
}

func TestNdjsonLogOmitsAbsentFields(t *testing.T) {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.Body().SetStr("bare")

	// No timestamps and no attributes at any level: both keys disappear.
	assert.Equal(t, `{"message":"bare"}`+"\n", marshalLogs(t, logs))
}

// TestNdjsonMetricZeroValueRetained is the omitempty regression guard: 0 is a
// legitimate reading and must survive to the output.
func TestNdjsonMetricZeroValueRetained(t *testing.T) {
	metrics := pmetric.NewMetrics()
	sm := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()

	gauge := sm.Metrics().AppendEmpty()
	gauge.SetName("zero_gauge")
	gauge.SetEmptyGauge().DataPoints().AppendEmpty().SetIntValue(0)

	histogram := sm.Metrics().AppendEmpty()
	histogram.SetName("zero_histogram")
	histogram.SetEmptyHistogram().DataPoints().AppendEmpty().SetCount(0)

	out, err := ndjsonMarshaler{}.MarshalMetrics(metrics)
	require.NoError(t, err)
	lines := splitLines(t, string(out))
	require.Len(t, lines, 2)
	assert.Contains(t, lines[0], `"asInt":0`)
	assert.Contains(t, lines[1], `"count":0`)
}

func TestNdjsonMetricIntValueIsJSONNumber(t *testing.T) {
	metrics := pmetric.NewMetrics()
	metric := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	metric.SetName("int_gauge")
	metric.SetEmptyGauge().DataPoints().AppendEmpty().SetIntValue(19)

	out, err := ndjsonMarshaler{}.MarshalMetrics(metrics)
	require.NoError(t, err)
	assert.Contains(t, string(out), `"asInt":19`)
	assert.NotContains(t, string(out), `"asInt":"19"`)
	// An int must not be rendered through float64 as 19.0 either.
	assert.NotContains(t, string(out), `"asInt":19.0`)
	assert.NotContains(t, string(out), `"asDouble"`)
}

func TestNdjsonMetricHistogram(t *testing.T) {
	metrics := pmetric.NewMetrics()
	metric := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	metric.SetName("http.server.duration")
	metric.SetUnit("s")
	dp := metric.SetEmptyHistogram().DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.Timestamp(1787617817596000000))
	dp.SetCount(42)
	dp.SetSum(1.37)
	dp.SetMin(0.002)
	dp.SetMax(0.41)
	dp.BucketCounts().FromRaw([]uint64{12, 20, 8, 2})
	dp.ExplicitBounds().FromRaw([]float64{0.01, 0.1, 1.0})

	out, err := ndjsonMarshaler{}.MarshalMetrics(metrics)
	require.NoError(t, err)
	assert.Equal(t,
		`{"name":"http.server.duration","unit":"s","type":"histogram",`+
			`"timeUnixNano":"1787617817596000000","count":42,"sum":1.37,"min":0.002,"max":0.41,`+
			`"bucketCounts":[12,20,8,2],"explicitBounds":[0.01,0.1,1]}`+"\n",
		string(out))
}

func TestNdjsonMetricHistogramOmitsUnsetSumMinMax(t *testing.T) {
	metrics := pmetric.NewMetrics()
	metric := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	metric.SetName("sparse_histogram")
	dp := metric.SetEmptyHistogram().DataPoints().AppendEmpty()
	dp.SetCount(3)

	out, err := ndjsonMarshaler{}.MarshalMetrics(metrics)
	require.NoError(t, err)
	assert.Contains(t, string(out), `"count":3`)
	assert.NotContains(t, string(out), `"sum"`)
	assert.NotContains(t, string(out), `"min"`)
	assert.NotContains(t, string(out), `"max"`)
}

func TestNdjsonMetricExponentialHistogram(t *testing.T) {
	metrics := pmetric.NewMetrics()
	metric := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	metric.SetName("expo")
	dp := metric.SetEmptyExponentialHistogram().DataPoints().AppendEmpty()
	dp.SetCount(10)
	dp.SetSum(5.5)
	dp.SetScale(2)
	dp.SetZeroCount(1)
	dp.Positive().SetOffset(3)
	dp.Positive().BucketCounts().FromRaw([]uint64{4, 5})

	line := unmarshalLine(t, strings.TrimSuffix(mustMarshalMetrics(t, metrics), "\n"))
	assert.Equal(t, "exponential_histogram", line["type"])
	assert.InDelta(t, float64(2), line["scale"], 0)
	assert.InDelta(t, float64(1), line["zeroCount"], 0)
	positive := line["positive"].(map[string]any)
	assert.InDelta(t, float64(3), positive["offset"], 0)
	assert.Equal(t, []any{float64(4), float64(5)}, positive["bucketCounts"])
	// No negative buckets were set, so the key is absent.
	assert.NotContains(t, line, "negative")
}

func TestNdjsonMetricSummary(t *testing.T) {
	metrics := pmetric.NewMetrics()
	metric := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	metric.SetName("rpc.duration")
	dp := metric.SetEmptySummary().DataPoints().AppendEmpty()
	dp.SetCount(100)
	dp.SetSum(12.5)
	qv := dp.QuantileValues().AppendEmpty()
	qv.SetQuantile(0.99)
	qv.SetValue(0.42)

	out := mustMarshalMetrics(t, metrics)
	assert.Contains(t, out, `"type":"summary"`)
	assert.Contains(t, out, `"quantileValues":[{"quantile":0.99,"value":0.42}]`)
}

func TestNdjsonMetricSkipsNaNAndNoRecordedValue(t *testing.T) {
	metrics := pmetric.NewMetrics()
	sm := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()

	nan := sm.Metrics().AppendEmpty()
	nan.SetName("staleness_marker")
	nan.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(math.NaN())

	noValue := sm.Metrics().AppendEmpty()
	noValue.SetName("no_recorded_value")
	noValueDP := noValue.SetEmptyGauge().DataPoints().AppendEmpty()
	noValueDP.SetIntValue(1)
	noValueDP.SetFlags(pmetric.DefaultDataPointFlags.WithNoRecordedValue(true))

	// A NaN histogram sum loses only that field; count and buckets survive.
	hist := sm.Metrics().AppendEmpty()
	hist.SetName("nan_sum_histogram")
	histDP := hist.SetEmptyHistogram().DataPoints().AppendEmpty()
	histDP.SetCount(4)
	histDP.SetSum(math.NaN())

	out, err := ndjsonMarshaler{}.MarshalMetrics(metrics)
	require.NoError(t, err)
	lines := splitLines(t, string(out))
	require.Len(t, lines, 1)
	assert.Contains(t, lines[0], `"name":"nan_sum_histogram"`)
	assert.Contains(t, lines[0], `"count":4`)
	assert.NotContains(t, lines[0], `"sum"`)
}

func TestNdjsonMarshalMultipleRecords(t *testing.T) {
	logs := plog.NewLogs()
	for i := range 2 {
		rl := logs.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutInt("resource.index", int64(i))
		for range 2 {
			sl := rl.ScopeLogs().AppendEmpty()
			for range 2 {
				sl.LogRecords().AppendEmpty().Body().SetStr("record")
			}
		}
	}

	out, err := ndjsonMarshaler{}.MarshalLogs(logs)
	require.NoError(t, err)
	assert.Equal(t, 8, strings.Count(string(out), "\n"))
	for _, line := range splitLines(t, string(out)) {
		var record map[string]any
		assert.NoError(t, json.Unmarshal([]byte(line), &record), "each line must parse on its own")
	}
}

// TestNdjsonMarshalerNonMutatingFlow guards against the marshaler writing back
// into the pipeline's data, which would corrupt every downstream exporter.
func TestNdjsonMarshalerNonMutatingFlow(t *testing.T) {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("shared", "resource")
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().Attributes().PutStr("shared", "scope")
	lr := sl.LogRecords().AppendEmpty()
	lr.Attributes().PutStr("shared", "record")
	lr.Body().SetStr("body")

	resourceBefore := rl.Resource().Attributes().AsRaw()
	scopeBefore := sl.Scope().Attributes().AsRaw()
	recordBefore := lr.Attributes().AsRaw()
	bodyBefore := lr.Body().AsRaw()

	_, err := ndjsonMarshaler{}.MarshalLogs(logs)
	require.NoError(t, err)

	t.Run("ResourceAttributesUnchanged", func(t *testing.T) {
		assert.Equal(t, resourceBefore, rl.Resource().Attributes().AsRaw())
	})
	t.Run("ScopeAttributesUnchanged", func(t *testing.T) {
		assert.Equal(t, scopeBefore, sl.Scope().Attributes().AsRaw())
	})
	t.Run("RecordAttributesUnchanged", func(t *testing.T) {
		assert.Equal(t, recordBefore, lr.Attributes().AsRaw())
	})
	t.Run("BodyUnchanged", func(t *testing.T) {
		assert.Equal(t, bodyBefore, lr.Body().AsRaw())
	})
}

func TestNdjsonMarshalerMultipleCallsSafe(t *testing.T) {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("k8s.namespace.name", "simba")
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.Body().SetStr("repeatable")
	lr.Attributes().PutStr("log.iostream", "stdout")

	first, err := ndjsonMarshaler{}.MarshalLogs(logs)
	require.NoError(t, err)
	for range 2 {
		again, err := ndjsonMarshaler{}.MarshalLogs(logs)
		require.NoError(t, err)
		assert.Equal(t, string(first), string(again))
	}
}

func TestNdjsonMarshalEmpty(t *testing.T) {
	m := ndjsonMarshaler{}

	logs, err := m.MarshalLogs(plog.NewLogs())
	assert.NoError(t, err)
	assert.Empty(t, logs)

	metrics, err := m.MarshalMetrics(pmetric.NewMetrics())
	assert.NoError(t, err)
	assert.Empty(t, metrics)

	traces, err := m.MarshalTraces(ptrace.NewTraces())
	assert.NoError(t, err)
	assert.Empty(t, traces)
}

func TestNdjsonTraceOmitsUnsetStatusAndParent(t *testing.T) {
	traces := ptrace.NewTraces()
	span := traces.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("root")

	out, err := ndjsonMarshaler{}.MarshalTraces(traces)
	require.NoError(t, err)
	assert.Equal(t, `{"name":"root"}`+"\n", string(out))
}

func TestNdjsonDoesNotEscapeHTML(t *testing.T) {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.Body().SetStr("GET /search?a=1&b=2 <ok>")

	line := marshalLogsToLine(t, logs)
	assert.Contains(t, line, `GET /search?a=1&b=2 <ok>`)
	// The stdlib default would have written these as \u0026 and \u003c.
	assert.NotContains(t, line, `\u0026`)
	assert.NotContains(t, line, `\u003c`)
}

func marshalLogs(t *testing.T, logs plog.Logs) string {
	t.Helper()
	out, err := ndjsonMarshaler{}.MarshalLogs(logs)
	require.NoError(t, err)
	return string(out)
}

// marshalLogsToLine marshals logs expected to produce exactly one line and
// returns it without the trailing newline.
func marshalLogsToLine(t *testing.T, logs plog.Logs) string {
	t.Helper()
	lines := splitLines(t, marshalLogs(t, logs))
	require.Len(t, lines, 1)
	return lines[0]
}

func mustMarshalMetrics(t *testing.T, metrics pmetric.Metrics) string {
	t.Helper()
	out, err := ndjsonMarshaler{}.MarshalMetrics(metrics)
	require.NoError(t, err)
	return string(out)
}

func splitLines(t *testing.T, out string) []string {
	t.Helper()
	require.True(t, out == "" || strings.HasSuffix(out, "\n"), "output must be newline terminated")
	return strings.Split(strings.TrimSuffix(out, "\n"), "\n")
}

func unmarshalLine(t *testing.T, line string) map[string]any {
	t.Helper()
	var record map[string]any
	require.NoError(t, json.Unmarshal([]byte(line), &record))
	return record
}

// TestNdjsonLogBodyRouting pins the whole discriminator contract: which key a
// body type lands in, and that exactly one of the three is ever present.
func TestNdjsonLogBodyRouting(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  func(pcommon.Value)
		want string // empty means no payload key at all
	}{
		{"str", func(v pcommon.Value) { v.SetStr("hello") }, `"message":"hello"`},
		{"int", func(v pcommon.Value) { v.SetInt(42) }, `"message":"42"`},
		{"double", func(v pcommon.Value) { v.SetDouble(1.5) }, `"message":"1.5"`},
		{"bool", func(v pcommon.Value) { v.SetBool(true) }, `"message":"true"`},
		{"bytes", func(v pcommon.Value) { v.SetEmptyBytes().FromRaw([]byte{0x01, 0x02}) }, `"message":"AQI="`},
		{"map", func(v pcommon.Value) { v.SetEmptyMap().PutStr("k", "v") }, `"body":{"k":"v"}`},
		{"slice", func(v pcommon.Value) { v.SetEmptySlice().AppendEmpty().SetStr("a") }, `"array":["a"]`},
		{"empty", func(pcommon.Value) {}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := plog.NewLogs()
			lr := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
			lr.SetTimestamp(pcommon.Timestamp(1787617744022570766))
			tc.set(lr.Body())

			line := marshalLogsToLine(t, logs)
			record := unmarshalLine(t, line)
			present := 0
			for _, k := range []string{"message", "body", "array"} {
				if _, ok := record[k]; ok {
					present++
				}
			}
			if tc.want == "" {
				assert.Zero(t, present, "an unset body must set no payload key")
				return
			}
			assert.Equal(t, 1, present, "exactly one payload key: it is the discriminator")
			assert.Contains(t, line, tc.want)
		})
	}
}

func TestNdjsonLogEmptyStringBodyOmitsMessage(t *testing.T) {
	logs := plog.NewLogs()
	lr := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.Timestamp(1787617744022570766))
	lr.Body().SetStr("")

	assert.Equal(t, `{"timeUnixNano":"1787617744022570766"}`+"\n", marshalLogs(t, logs))
}

// TestNdjsonLogBodyStripsUnsetAndEmptyContainers is shaped like a k8sobjects
// Event: a managedFields entry whose fieldsV1 tree is nothing but empty
// containers, alongside fields the API server left unset and fields whose value
// is a legitimate zero.
func TestNdjsonLogBodyStripsUnsetAndEmptyContainers(t *testing.T) {
	logs := plog.NewLogs()
	lr := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()

	body := lr.Body().SetEmptyMap()
	body.PutStr("reason", "RemovingNode")
	body.PutInt("deprecatedCount", 0)
	body.PutBool("deprecated", false)
	body.PutStr("note", "")
	body.PutEmpty("eventTime")
	metadata := body.PutEmptyMap("metadata")
	metadata.PutStr("name", "ip-10-50-93-165.18cf1e781a22030c")
	metadata.PutEmpty("deletionTimestamp")
	entry := metadata.PutEmptySlice("managedFields").AppendEmpty().SetEmptyMap()
	entry.PutStr("manager", "kube-controller-manager")
	entry.PutStr("operation", "Update")
	fields := entry.PutEmptyMap("fieldsV1")
	// The real exports use empty kvlists here rather than unset values, so this
	// subtree collapses on the empty-container rule, not the null rule.
	fields.PutEmptyMap("f:involvedObject")
	fields.PutEmptyMap("f:reason")
	fields.PutEmptyMap("f:source").PutEmptyMap("f:component")

	line := marshalLogsToLine(t, logs)
	assert.NotContains(t, line, "null", "no null may reach the output")
	assert.NotContains(t, line, "{}", "no empty object may reach the output")

	got, ok := unmarshalLine(t, line)["body"].(map[string]any)
	require.True(t, ok, "a kvlist body must land in body")

	assert.Equal(t, "RemovingNode", got["reason"])
	// Zero values are data, not absence.
	assert.InDelta(t, float64(0), got["deprecatedCount"], 0)
	assert.Equal(t, false, got["deprecated"])
	assert.Empty(t, got["note"], "an empty string is data, not absence")
	assert.NotContains(t, got, "eventTime")

	metadataGot := got["metadata"].(map[string]any)
	assert.Equal(t, "ip-10-50-93-165.18cf1e781a22030c", metadataGot["name"])
	assert.NotContains(t, metadataGot, "deletionTimestamp")

	managed := metadataGot["managedFields"].([]any)
	require.Len(t, managed, 1)
	entryGot := managed[0].(map[string]any)
	assert.Equal(t, "kube-controller-manager", entryGot["manager"])
	assert.NotContains(t, entryGot, "fieldsV1", "every leaf was empty, so the subtree collapses bottom up")
}

func TestNdjsonLogBodyStrippedToNothingOmitsKey(t *testing.T) {
	logs := plog.NewLogs()
	lr := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	body := lr.Body().SetEmptyMap()
	body.PutEmpty("a")
	body.PutEmptyMap("b").PutEmptySlice("c")

	assert.Equal(t, "{}\n", marshalLogs(t, logs))
}

func TestNdjsonLogArrayStripsAbsentElements(t *testing.T) {
	logs := plog.NewLogs()
	lr := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	s := lr.Body().SetEmptySlice()
	s.AppendEmpty().SetStr("a")
	s.AppendEmpty()           // unset
	s.AppendEmpty().SetInt(0) // a zero, which must survive
	s.AppendEmpty().SetEmptyMap()

	assert.Contains(t, marshalLogsToLine(t, logs), `"array":["a",0]`)
}

func TestNdjsonLogAllAbsentArrayOmitsKey(t *testing.T) {
	logs := plog.NewLogs()
	lr := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	s := lr.Body().SetEmptySlice()
	s.AppendEmpty()
	s.AppendEmpty()

	assert.Equal(t, "{}\n", marshalLogs(t, logs))
}

// TestNdjsonLogBodyDeepNesting guards the bottom-up ordering: a chain survives
// only if something at the bottom of it does.
func TestNdjsonLogBodyDeepNesting(t *testing.T) {
	t.Run("SurvivesWhenLeafHasValue", func(t *testing.T) {
		logs := plog.NewLogs()
		lr := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		lr.Body().SetEmptyMap().PutEmptyMap("a").PutEmptyMap("b").PutEmptyMap("c").PutStr("d", "deep")

		assert.Contains(t, marshalLogsToLine(t, logs), `"body":{"a":{"b":{"c":{"d":"deep"}}}}`)
	})

	t.Run("CollapsesWhenLeafIsEmpty", func(t *testing.T) {
		logs := plog.NewLogs()
		lr := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		lr.Body().SetEmptyMap().PutEmptyMap("a").PutEmptyMap("b").PutEmptyMap("c").PutEmptyMap("d")

		assert.Equal(t, "{}\n", marshalLogs(t, logs))
	})
}

// TestNdjsonLogStripDoesNotMutatePdata covers the strip specifically: it is the
// only part of this marshaler that mutates anything, and it must only ever
// mutate the fresh maps AsRaw hands back.
func TestNdjsonLogStripDoesNotMutatePdata(t *testing.T) {
	logs := plog.NewLogs()
	lr := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	body := lr.Body().SetEmptyMap()
	body.PutStr("keep", "yes")
	body.PutEmpty("drop")
	body.PutEmptyMap("nested").PutEmpty("deep")

	before := lr.Body().AsRaw()
	_, err := ndjsonMarshaler{}.MarshalLogs(logs)
	require.NoError(t, err)

	assert.Equal(t, before, lr.Body().AsRaw(), "the source body must be untouched")
	assert.Contains(t, before, "drop", "the snapshot itself must still hold the stripped key")
}

func TestNdjsonAttributesStripped(t *testing.T) {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("kept", "resource")
	rl.Resource().Attributes().PutEmpty("resource.unset")
	rl.Resource().Attributes().PutEmptyMap("resource.empty")
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.Body().SetStr("x")
	lr.Attributes().PutStr("record.set", "yes")
	lr.Attributes().PutEmpty("record.unset")
	// An unset record attribute must not shadow the resource attribute it would
	// otherwise overwrite.
	lr.Attributes().PutEmpty("kept")

	line := marshalLogsToLine(t, logs)
	assert.NotContains(t, line, "null")

	attrs := unmarshalLine(t, line)["attributes"].(map[string]any)
	assert.Equal(t, "resource", attrs["kept"])
	assert.Equal(t, "yes", attrs["record.set"])
	assert.NotContains(t, attrs, "resource.unset")
	assert.NotContains(t, attrs, "resource.empty")
	assert.NotContains(t, attrs, "record.unset")
}

// TestNdjsonMetricValueTypeRouting pins the value-key contract: an int lands in
// asInt, a double in asDouble, never both, and an unset data point emits
// nothing. Those cases are exhaustive over the OTLP value oneof.
func TestNdjsonMetricValueTypeRouting(t *testing.T) {
	for _, tc := range []struct {
		name    string
		set     func(pmetric.NumberDataPoint)
		want    string
		absent  []string
		present bool
	}{
		{"int", func(dp pmetric.NumberDataPoint) { dp.SetIntValue(19) }, `"asInt":19`, []string{"asDouble"}, true},
		{"int zero", func(dp pmetric.NumberDataPoint) { dp.SetIntValue(0) }, `"asInt":0`, []string{"asDouble"}, true},
		{"double", func(dp pmetric.NumberDataPoint) { dp.SetDoubleValue(1.5) }, `"asDouble":1.5`, []string{"asInt"}, true},
		{"double zero", func(dp pmetric.NumberDataPoint) { dp.SetDoubleValue(0) }, `"asDouble":0`, []string{"asInt"}, true},
		{"unset", func(pmetric.NumberDataPoint) {}, "", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metrics := pmetric.NewMetrics()
			metric := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			metric.SetName("g")
			tc.set(metric.SetEmptyGauge().DataPoints().AppendEmpty())

			out, err := ndjsonMarshaler{}.MarshalMetrics(metrics)
			require.NoError(t, err)

			if !tc.present {
				assert.Empty(t, out, "a data point with no value emits no line")
				return
			}
			assert.Contains(t, string(out), tc.want)
			for _, k := range tc.absent {
				assert.NotContains(t, string(out), `"`+k+`"`, "exactly one value key: it is the discriminator")
			}
		})
	}
}

// TestNdjsonMetricDoubleKeepsFractionalZero guards the pointer choice: a double
// of 0 must stay in asDouble rather than being dropped by omitempty or
// collapsing into the integer key.
func TestNdjsonMetricDoubleKeepsFractionalZero(t *testing.T) {
	metrics := pmetric.NewMetrics()
	sm := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()

	zero := sm.Metrics().AppendEmpty()
	zero.SetName("zero_double")
	zero.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(0)

	out, err := ndjsonMarshaler{}.MarshalMetrics(metrics)
	require.NoError(t, err)
	assert.Contains(t, string(out), `"asDouble":0`)
	assert.NotContains(t, string(out), `"asInt"`)
}
