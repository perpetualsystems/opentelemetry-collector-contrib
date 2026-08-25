// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awss3exporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awss3exporter"

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"maps"
	"math"
	"strconv"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// ndjsonFileFormat is the file extension used for objects written by the ndjson
// marshaler. It intentionally differs from the marshaler's configuration name
// ("ndjson"): ".jsonl" is the extension downstream tooling recognizes.
const ndjsonFileFormat = "jsonl"

// ndjsonMarshaler writes one JSON object per line (newline delimited JSON).
// Every emitted line is self contained: resource and scope attributes are
// flattened onto each log record, metric data point and span, so a consumer
// never has to walk back up the OTLP envelope to interpret a record.
//
// Attribute values keep their native JSON types (numbers stay numbers, nested
// maps stay objects) rather than being stringified.
type ndjsonMarshaler struct{}

func newNdjsonMarshaler() ndjsonMarshaler {
	return ndjsonMarshaler{}
}

func (ndjsonMarshaler) format() string {
	return ndjsonFileFormat
}

// The record types below use struct fields rather than maps because
// encoding/json preserves struct field order but sorts map keys, and the field
// order is part of the format.
//
// Fields that may legitimately hold a zero value use a pointer or `any`: for
// `any`, omitempty tests for nil rather than for the zero value, so a
// value of 0 is still emitted. Do not replace these with omitzero, which would
// drop a genuine `"value":0` or `"count":0`.
//
// ndjsonLogRecord.Message is the deliberate exception: it is a plain string,
// because the body has already been rendered to text and so has no zero value
// to protect. An int body of 0 renders as "0" and survives; only a genuinely
// empty string body is dropped, which is the intent.

// ndjsonLogRecord carries exactly one of Message, Body or Array, chosen by the
// type of the log body. Which key is present is the discriminator, and the
// concrete field types keep each key's JSON type stable across every line, so
// the archive can be read with a fixed schema.
type ndjsonLogRecord struct {
	TimeUnixNano string         `json:"timeUnixNano,omitempty"`
	Message      string         `json:"message,omitempty"`
	Body         map[string]any `json:"body,omitempty"`
	Array        []any          `json:"array,omitempty"`
	Attributes   map[string]any `json:"attributes,omitempty"`
}

type ndjsonMetricRecord struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Unit        string `json:"unit,omitempty"`
	Type        string `json:"type"`

	StartTimeUnixNano string `json:"startTimeUnixNano,omitempty"`
	TimeUnixNano      string `json:"timeUnixNano,omitempty"`

	// Gauge and sum data points.
	Value any `json:"value,omitempty"`

	// Histogram, exponential histogram and summary data points.
	Count          *uint64   `json:"count,omitempty"`
	Sum            *float64  `json:"sum,omitempty"`
	Min            *float64  `json:"min,omitempty"`
	Max            *float64  `json:"max,omitempty"`
	BucketCounts   []uint64  `json:"bucketCounts,omitempty"`
	ExplicitBounds []float64 `json:"explicitBounds,omitempty"`

	// Exponential histogram data points.
	Scale         *int32             `json:"scale,omitempty"`
	ZeroCount     *uint64            `json:"zeroCount,omitempty"`
	ZeroThreshold *float64           `json:"zeroThreshold,omitempty"`
	Positive      *ndjsonExpoBuckets `json:"positive,omitempty"`
	Negative      *ndjsonExpoBuckets `json:"negative,omitempty"`

	// Summary data points.
	QuantileValues []ndjsonQuantile `json:"quantileValues,omitempty"`

	Attributes map[string]any `json:"attributes,omitempty"`
}

type ndjsonExpoBuckets struct {
	Offset       int32    `json:"offset"`
	BucketCounts []uint64 `json:"bucketCounts,omitempty"`
}

type ndjsonQuantile struct {
	Quantile float64 `json:"quantile"`
	Value    float64 `json:"value"`
}

type ndjsonSpanRecord struct {
	TraceID           string            `json:"traceId,omitempty"`
	SpanID            string            `json:"spanId,omitempty"`
	ParentSpanID      string            `json:"parentSpanId,omitempty"`
	Name              string            `json:"name"`
	Kind              string            `json:"kind,omitempty"`
	StartTimeUnixNano string            `json:"startTimeUnixNano,omitempty"`
	EndTimeUnixNano   string            `json:"endTimeUnixNano,omitempty"`
	Status            *ndjsonSpanStatus `json:"status,omitempty"`
	Attributes        map[string]any    `json:"attributes,omitempty"`
}

type ndjsonSpanStatus struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

// newNdjsonEncoder returns an encoder that writes one JSON value per line.
// json.Encoder.Encode already appends the newline, which is exactly the
// newline delimited JSON framing.
//
// HTML escaping is disabled so that URLs and other markup common in log bodies
// are written literally instead of as < escapes.
func newNdjsonEncoder(buf *bytes.Buffer) *json.Encoder {
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	return enc
}

// mergeAttributes returns base overlaid with overlay, with overlay winning on
// conflicting keys. The input pdata is only read: pcommon.Value.AsRaw
// allocates fresh Go values, so nothing is mutated or aliased back into the
// pipeline's data.
//
// Overlay values are stripped as they are inserted, and an absent one is
// skipped rather than written: an unset attribute carries no information, so it
// must not shadow the resource attribute of the same name that it would
// otherwise overwrite.
func mergeAttributes(base map[string]any, overlay pcommon.Map) map[string]any {
	if overlay.Len() == 0 {
		// Safe to alias: the result is only ever JSON encoded, never written to.
		return base
	}
	out := make(map[string]any, len(base)+overlay.Len())
	maps.Copy(out, base)
	for k, v := range overlay.All() {
		raw := ndjsonStripValue(v.AsRaw())
		if ndjsonIsAbsent(raw) {
			continue
		}
		out[k] = raw
	}
	return out
}

// ndjsonResourceAttributes renders a resource's attributes once per resource.
// The result is aliased by mergeAttributes for every record underneath, so
// stripping it here strips it for all of them.
func ndjsonResourceAttributes(res pcommon.Resource) map[string]any {
	attrs := res.Attributes().AsRaw()
	ndjsonStripMap(attrs)
	return attrs
}

// ndjsonLogBody routes a log body to exactly one of the three payload fields.
// An unset body sets none of them.
//
// Scalars go through Value.AsString, which renders a string verbatim, numbers
// and bools as text, and bytes as base64. That also renders a non-finite double
// as "NaN" or "Infinity" rather than failing the encode, which is what the
// metric path has to guard against separately: a log line saying NaN is
// information, whereas a metric with no value is noise.
//
// Maps and slices are stripped first. OTLP cannot express "absent" inside a
// kvlist, so an unset entry arrives from AsRaw as a nil that would otherwise be
// written as a JSON null.
func ndjsonLogBody(v pcommon.Value) (message string, body map[string]any, array []any) {
	switch v.Type() {
	case pcommon.ValueTypeEmpty:
		return "", nil, nil
	case pcommon.ValueTypeMap:
		m := v.Map().AsRaw()
		ndjsonStripMap(m)
		return "", m, nil
	case pcommon.ValueTypeSlice:
		return "", nil, ndjsonStripSlice(v.Slice().AsRaw())
	default:
		// Str, Int, Double, Bool, Bytes, and any scalar pdata adds later.
		return v.AsString(), nil, nil
	}
}

// ndjsonStripMap deletes every key whose value carries no information. It
// recurses before testing, so a nested container that the strip empties is
// itself deleted, bottom up: a Kubernetes object's fieldsV1 subtree, whose
// leaves are all empty, disappears entirely.
//
// The map comes from AsRaw, which allocates fresh at every nesting level, so
// mutating it in place cannot reach the pipeline's data. Deleting during a
// range is defined behavior and the deleted key is not revisited.
func ndjsonStripMap(m map[string]any) {
	for k, v := range m {
		v = ndjsonStripValue(v)
		if ndjsonIsAbsent(v) {
			delete(m, k)
			continue
		}
		m[k] = v
	}
}

// ndjsonStripSlice filters in place, which is why it must return: dropping an
// element shortens the slice. The write index never overtakes the read index,
// so it only ever overwrites elements already copied out.
//
// Dropping an element shifts those after it, so array indices are not
// positionally stable. See the ndjson section of README.md.
func ndjsonStripSlice(s []any) []any {
	out := s[:0]
	for _, v := range s {
		v = ndjsonStripValue(v)
		if ndjsonIsAbsent(v) {
			continue
		}
		out = append(out, v)
	}
	return out
}

func ndjsonStripValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		ndjsonStripMap(t)
		return t
	case []any:
		return ndjsonStripSlice(t)
	default:
		// Includes []byte, which AsRaw returns for a bytes value. That is a
		// scalar, base64 encoded by encoding/json, and must not be treated as
		// a container.
		return v
	}
}

// ndjsonIsAbsent reports whether a value carries no information: a null, or a
// container with nothing in it. A container that arrived empty is dropped just
// like one the strip emptied.
//
// Zero values are information: 0, false and "" all reach the default arm and
// survive, so the strip can never turn a recorded value into a missing key.
func ndjsonIsAbsent(v any) bool {
	switch t := v.(type) {
	case nil:
		return true
	case map[string]any:
		return len(t) == 0
	case []any:
		return len(t) == 0
	default:
		return false
	}
}

// ndjsonTimestamp renders nanoseconds as a decimal string. OTLP/JSON does the
// same, because nanosecond timestamps exceed 2^53 and would lose precision in
// any consumer that parses JSON numbers as doubles. A zero timestamp renders as
// the empty string so that omitempty drops the field.
func ndjsonTimestamp(ts pcommon.Timestamp) string {
	if ts == 0 {
		return ""
	}
	return strconv.FormatUint(uint64(ts), 10)
}

// ndjsonFloat returns nil for values encoding/json cannot represent. Metrics
// scraped from Prometheus carry NaN for staleness markers, and a single one
// would otherwise fail the whole batch.
func ndjsonFloat(f float64) any {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nil
	}
	return f
}

func ndjsonFloatPtr(f float64) *float64 {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nil
	}
	return &f
}

func (ndjsonMarshaler) MarshalLogs(ld plog.Logs) ([]byte, error) {
	buf := bytes.Buffer{}
	enc := newNdjsonEncoder(&buf)

	for _, rl := range ld.ResourceLogs().All() {
		resourceAttrs := ndjsonResourceAttributes(rl.Resource())
		for _, sl := range rl.ScopeLogs().All() {
			scopeAttrs := mergeAttributes(resourceAttrs, sl.Scope().Attributes())
			for _, lr := range sl.LogRecords().All() {
				// Fall back to the observed timestamp: receivers such as
				// filelog set only that one when no timestamp parser is
				// configured, and a line with no time at all is useless.
				ts := lr.Timestamp()
				if ts == 0 {
					ts = lr.ObservedTimestamp()
				}
				message, body, array := ndjsonLogBody(lr.Body())
				record := ndjsonLogRecord{
					TimeUnixNano: ndjsonTimestamp(ts),
					Message:      message,
					Body:         body,
					Array:        array,
					Attributes:   mergeAttributes(scopeAttrs, lr.Attributes()),
				}
				if err := enc.Encode(record); err != nil {
					return nil, err
				}
			}
		}
	}
	return buf.Bytes(), nil
}

func (m ndjsonMarshaler) MarshalMetrics(md pmetric.Metrics) ([]byte, error) {
	buf := bytes.Buffer{}
	enc := newNdjsonEncoder(&buf)

	for _, rm := range md.ResourceMetrics().All() {
		resourceAttrs := ndjsonResourceAttributes(rm.Resource())
		for _, sm := range rm.ScopeMetrics().All() {
			scopeAttrs := mergeAttributes(resourceAttrs, sm.Scope().Attributes())
			for _, metric := range sm.Metrics().All() {
				if err := m.encodeMetric(enc, metric, scopeAttrs); err != nil {
					return nil, err
				}
			}
		}
	}
	return buf.Bytes(), nil
}

// encodeMetric writes one line per data point, lifting the metric's identity
// onto each of them so every line stands alone.
func (m ndjsonMarshaler) encodeMetric(enc *json.Encoder, metric pmetric.Metric, scopeAttrs map[string]any) error {
	// Template carrying the fields shared by every data point of this metric.
	// Each data point takes a struct copy and fills in its own fields.
	template := ndjsonMetricRecord{
		Name:        metric.Name(),
		Description: metric.Description(),
		Unit:        metric.Unit(),
		Type:        ndjsonMetricType(metric.Type()),
	}

	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		return m.encodeNumberDataPoints(enc, metric.Gauge().DataPoints(), template, scopeAttrs)
	case pmetric.MetricTypeSum:
		// NOTE: aggregationTemporality and isMonotonic are deliberately not
		// emitted, which makes a delta counter indistinguishable from a
		// cumulative one. See the ndjson section of README.md.
		return m.encodeNumberDataPoints(enc, metric.Sum().DataPoints(), template, scopeAttrs)
	case pmetric.MetricTypeHistogram:
		return m.encodeHistogramDataPoints(enc, metric.Histogram().DataPoints(), template, scopeAttrs)
	case pmetric.MetricTypeExponentialHistogram:
		return m.encodeExponentialHistogramDataPoints(enc, metric.ExponentialHistogram().DataPoints(), template, scopeAttrs)
	case pmetric.MetricTypeSummary:
		return m.encodeSummaryDataPoints(enc, metric.Summary().DataPoints(), template, scopeAttrs)
	default:
		// MetricTypeEmpty, and any type added to pdata after this was written.
		return nil
	}
}

func ndjsonMetricType(t pmetric.MetricType) string {
	switch t {
	case pmetric.MetricTypeGauge:
		return "gauge"
	case pmetric.MetricTypeSum:
		return "sum"
	case pmetric.MetricTypeHistogram:
		return "histogram"
	case pmetric.MetricTypeExponentialHistogram:
		return "exponential_histogram"
	case pmetric.MetricTypeSummary:
		return "summary"
	default:
		return "unknown"
	}
}

func (ndjsonMarshaler) encodeNumberDataPoints(enc *json.Encoder, dps pmetric.NumberDataPointSlice, template ndjsonMetricRecord, scopeAttrs map[string]any) error {
	for _, dp := range dps.All() {
		if dp.Flags().NoRecordedValue() {
			continue
		}
		var value any
		switch dp.ValueType() {
		case pmetric.NumberDataPointValueTypeInt:
			value = dp.IntValue()
		case pmetric.NumberDataPointValueTypeDouble:
			value = ndjsonFloat(dp.DoubleValue())
		default:
			continue
		}
		if value == nil {
			// A non-finite reading. A line carrying identity but no value is
			// noise, so drop the data point entirely.
			continue
		}

		record := template
		record.StartTimeUnixNano = ndjsonTimestamp(dp.StartTimestamp())
		record.TimeUnixNano = ndjsonTimestamp(dp.Timestamp())
		record.Value = value
		record.Attributes = mergeAttributes(scopeAttrs, dp.Attributes())

		if err := enc.Encode(record); err != nil {
			return err
		}
	}
	return nil
}

func (ndjsonMarshaler) encodeHistogramDataPoints(enc *json.Encoder, dps pmetric.HistogramDataPointSlice, template ndjsonMetricRecord, scopeAttrs map[string]any) error {
	for _, dp := range dps.All() {
		if dp.Flags().NoRecordedValue() {
			continue
		}

		record := template
		record.StartTimeUnixNano = ndjsonTimestamp(dp.StartTimestamp())
		record.TimeUnixNano = ndjsonTimestamp(dp.Timestamp())
		count := dp.Count()
		record.Count = &count
		if dp.HasSum() {
			record.Sum = ndjsonFloatPtr(dp.Sum())
		}
		if dp.HasMin() {
			record.Min = ndjsonFloatPtr(dp.Min())
		}
		if dp.HasMax() {
			record.Max = ndjsonFloatPtr(dp.Max())
		}
		record.BucketCounts = dp.BucketCounts().AsRaw()
		record.ExplicitBounds = dp.ExplicitBounds().AsRaw()
		record.Attributes = mergeAttributes(scopeAttrs, dp.Attributes())

		if err := enc.Encode(record); err != nil {
			return err
		}
	}
	return nil
}

func (ndjsonMarshaler) encodeExponentialHistogramDataPoints(enc *json.Encoder, dps pmetric.ExponentialHistogramDataPointSlice, template ndjsonMetricRecord, scopeAttrs map[string]any) error {
	for _, dp := range dps.All() {
		if dp.Flags().NoRecordedValue() {
			continue
		}

		record := template
		record.StartTimeUnixNano = ndjsonTimestamp(dp.StartTimestamp())
		record.TimeUnixNano = ndjsonTimestamp(dp.Timestamp())
		count := dp.Count()
		record.Count = &count
		if dp.HasSum() {
			record.Sum = ndjsonFloatPtr(dp.Sum())
		}
		if dp.HasMin() {
			record.Min = ndjsonFloatPtr(dp.Min())
		}
		if dp.HasMax() {
			record.Max = ndjsonFloatPtr(dp.Max())
		}
		scale := dp.Scale()
		record.Scale = &scale
		zeroCount := dp.ZeroCount()
		record.ZeroCount = &zeroCount
		zeroThreshold := dp.ZeroThreshold()
		record.ZeroThreshold = &zeroThreshold
		record.Positive = ndjsonExpoBucketsOf(dp.Positive())
		record.Negative = ndjsonExpoBucketsOf(dp.Negative())
		record.Attributes = mergeAttributes(scopeAttrs, dp.Attributes())

		if err := enc.Encode(record); err != nil {
			return err
		}
	}
	return nil
}

func ndjsonExpoBucketsOf(b pmetric.ExponentialHistogramDataPointBuckets) *ndjsonExpoBuckets {
	if b.BucketCounts().Len() == 0 {
		return nil
	}
	return &ndjsonExpoBuckets{
		Offset:       b.Offset(),
		BucketCounts: b.BucketCounts().AsRaw(),
	}
}

func (ndjsonMarshaler) encodeSummaryDataPoints(enc *json.Encoder, dps pmetric.SummaryDataPointSlice, template ndjsonMetricRecord, scopeAttrs map[string]any) error {
	for _, dp := range dps.All() {
		if dp.Flags().NoRecordedValue() {
			continue
		}

		record := template
		record.StartTimeUnixNano = ndjsonTimestamp(dp.StartTimestamp())
		record.TimeUnixNano = ndjsonTimestamp(dp.Timestamp())
		count := dp.Count()
		record.Count = &count
		record.Sum = ndjsonFloatPtr(dp.Sum())
		if qvs := dp.QuantileValues(); qvs.Len() > 0 {
			quantiles := make([]ndjsonQuantile, 0, qvs.Len())
			for _, qv := range qvs.All() {
				value := ndjsonFloat(qv.Value())
				if value == nil {
					continue
				}
				quantiles = append(quantiles, ndjsonQuantile{
					Quantile: qv.Quantile(),
					Value:    qv.Value(),
				})
			}
			record.QuantileValues = quantiles
		}
		record.Attributes = mergeAttributes(scopeAttrs, dp.Attributes())

		if err := enc.Encode(record); err != nil {
			return err
		}
	}
	return nil
}

func (ndjsonMarshaler) MarshalTraces(td ptrace.Traces) ([]byte, error) {
	buf := bytes.Buffer{}
	enc := newNdjsonEncoder(&buf)

	for _, rs := range td.ResourceSpans().All() {
		resourceAttrs := ndjsonResourceAttributes(rs.Resource())
		for _, ss := range rs.ScopeSpans().All() {
			scopeAttrs := mergeAttributes(resourceAttrs, ss.Scope().Attributes())
			for _, span := range ss.Spans().All() {
				record := ndjsonSpanRecord{
					TraceID:           ndjsonTraceID(span.TraceID()),
					SpanID:            ndjsonSpanID(span.SpanID()),
					ParentSpanID:      ndjsonSpanID(span.ParentSpanID()),
					Name:              span.Name(),
					Kind:              ndjsonSpanKind(span.Kind()),
					StartTimeUnixNano: ndjsonTimestamp(span.StartTimestamp()),
					EndTimeUnixNano:   ndjsonTimestamp(span.EndTimestamp()),
					Status:            ndjsonStatusOf(span.Status()),
					Attributes:        mergeAttributes(scopeAttrs, span.Attributes()),
				}
				if err := enc.Encode(record); err != nil {
					return nil, err
				}
			}
		}
	}
	return buf.Bytes(), nil
}

func ndjsonTraceID(id pcommon.TraceID) string {
	if id.IsEmpty() {
		return ""
	}
	return hex.EncodeToString(id[:])
}

func ndjsonSpanID(id pcommon.SpanID) string {
	if id.IsEmpty() {
		return ""
	}
	return hex.EncodeToString(id[:])
}

// ndjsonSpanKind renders the lowercase short form rather than OTLP/JSON's
// SPAN_KIND_SERVER spelling, to stay consistent with the lowercase metric type.
func ndjsonSpanKind(kind ptrace.SpanKind) string {
	switch kind {
	case ptrace.SpanKindInternal:
		return "internal"
	case ptrace.SpanKindServer:
		return "server"
	case ptrace.SpanKindClient:
		return "client"
	case ptrace.SpanKindProducer:
		return "producer"
	case ptrace.SpanKindConsumer:
		return "consumer"
	default:
		return ""
	}
}

func ndjsonStatusOf(status ptrace.Status) *ndjsonSpanStatus {
	if status.Code() == ptrace.StatusCodeUnset && status.Message() == "" {
		return nil
	}
	return &ndjsonSpanStatus{
		Code:    ndjsonStatusCode(status.Code()),
		Message: status.Message(),
	}
}

func ndjsonStatusCode(code ptrace.StatusCode) string {
	switch code {
	case ptrace.StatusCodeOk:
		return "ok"
	case ptrace.StatusCodeError:
		return "error"
	default:
		return "unset"
	}
}
