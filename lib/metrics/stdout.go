// Copyright (c) 2014 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, sub to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package metrics

import (
	"encoding/json"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/gabs/v2"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeStdout] = TypeSpec{
		constructor: NewStdout,
		description: `
It is possible to expose metrics without an aggregator service while running in
serverless mode by having Benthos output metrics as JSON objects to stdout.  This
is useful if you do not have Prometheus or Statsd endpoints and you cannot query
the Benthos API for statistics (due to the short lived nature of serverless
invocations).

A series of JSON objects are emitted (one per line) grouped by the
input/processor/output instance.  Separation into individual JSON objects instead
of a single monolithic object allows for easy ingestion into document stores such
as Elasticsearch. `,
	}
}

//------------------------------------------------------------------------------

// StdoutConfig contains configuration parameters for the Stdout metrics
// aggregator.
type StdoutConfig struct {
	StaticFields map[string]interface{} `json:"static_fields" yaml:"static_fields"`
}

// NewStdoutConfig returns a new StdoutConfig with default values.
func NewStdoutConfig() StdoutConfig {
	return StdoutConfig{
		StaticFields: map[string]interface{}{
			"@service": "benthos",
		},
	}
}

//------------------------------------------------------------------------------

// Stdout is an object with capability to hold internal stats and emit them as
// individual JSON objects via stdout.
type Stdout struct {
	local        *Local
	timestamp    time.Time
	log          log.Modular
	staticFields []byte
}

// NewStdout creates and returns a new Stdout metric object.
func NewStdout(config Config, opts ...func(Type)) (Type, error) {
	t := &Stdout{
		local:     NewLocal(),
		timestamp: time.Now(),
	}

	//TODO: add field interloperation here
	sf, err := json.Marshal(config.Stdout.StaticFields)
	if err != nil {
		return t, fmt.Errorf("static_fields invalid format")
	}
	t.staticFields = sf

	for _, opt := range opts {
		opt(t)
	}
	return t, nil
}

//------------------------------------------------------------------------------

// writeMetric prints a metric object with any configured extras merged in to
// t.
func (s *Stdout) writeMetric(metricSet *gabs.Container) {
	base, _ := gabs.ParseJSON(s.staticFields)
	base.SetP(time.Now().Format(time.RFC3339), "@timestamp")
	base.Merge(metricSet)

	fmt.Printf("%s\n", base.String())
}

// publishMetrics
func (s *Stdout) publishMetrics() {
	counterObjs := make(map[string]*gabs.Container)

	counters := s.local.GetCounters()
	s.constructMetrics(counterObjs, counters)

	timings := s.local.GetTimings()
	s.constructMetrics(counterObjs, timings)

	system := make(map[string]int64)
	uptime := time.Since(s.timestamp).Milliseconds()
	goroutines := runtime.NumGoroutine()
	system["system.uptime"] = uptime
	system["system.goroutines"] = int64(goroutines)
	s.constructMetrics(counterObjs, system)

	for _, o := range counterObjs {
		s.writeMetric(o)
	}
}

// constructMetrics groups individual Benthos metrics contained in a map into
// a container for each component instance.  For example,
// pipeline.processor.1.count and pipeline.processor.1.error would be grouped
// into a single pipeline.processor.1 object.
func (s *Stdout) constructMetrics(co map[string]*gabs.Container, metrics map[string]int64) {
	for k, v := range metrics {
		kParts := strings.Split(k, ".")
		var objKey string
		var valKey string
		// walk key parts backwards building up objects of instances of processor/broker/etc
		for i := len(kParts) - 1; i >= 0; i-- {
			if _, err := strconv.Atoi(kParts[i]); err == nil {
				// part is a reference to an index of a processor/broker/etc
				objKey = strings.Join(kParts[:i+1], ".")
				valKey = strings.Join(kParts[i+1:], ".")
				break
			}
		}

		if objKey == "" {
			// key is not referencing an 'instance' of a processor/broker/etc
			objKey = kParts[0]
			valKey = strings.Join(kParts[0:], ".")
		}

		_, exists := co[objKey]
		if !exists {
			co[objKey] = gabs.New()
			co[objKey].SetP(objKey, "metric")
			co[objKey].SetP(kParts[0], "component")

		}
		co[objKey].SetP(v, valKey)
	}
}

// GetCounter returns a stat counter object for a path.
func (s *Stdout) GetCounter(path string) StatCounter {
	return s.local.GetCounter(path)
}

// GetCounterVec returns a stat counter object for a path with the labels
// discarded.
func (s *Stdout) GetCounterVec(path string, n []string) StatCounterVec {
	return fakeCounterVec(func([]string) StatCounter {
		return s.local.GetCounter(path)
	})
}

// GetTimer returns a stat timer object for a path.
func (s *Stdout) GetTimer(path string) StatTimer {
	return s.local.GetTimer(path)
}

// GetTimerVec returns a stat timer object for a path with the labels
// discarded.
func (s *Stdout) GetTimerVec(path string, n []string) StatTimerVec {
	return fakeTimerVec(func([]string) StatTimer {
		return s.local.GetTimer(path)
	})
}

// GetGauge returns a stat gauge object for a path.
func (s *Stdout) GetGauge(path string) StatGauge {
	return s.local.GetGauge(path)
}

// GetGaugeVec returns a stat timer object for a path with the labels
// discarded.
func (s *Stdout) GetGaugeVec(path string, n []string) StatGaugeVec {
	return fakeGaugeVec(func([]string) StatGauge {
		return s.local.GetGauge(path)
	})
}

// SetLogger does nothing.
func (s *Stdout) SetLogger(log log.Modular) {
	s.log = log
}

// Close stops the Stdout object from aggregating metrics and does a publish
// (write to stdout) of metrics.
func (s *Stdout) Close() error {
	s.publishMetrics()

	return nil
}

//------------------------------------------------------------------------------
