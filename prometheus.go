package wpgx

import (
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

type metricSet struct {
	AppName  string
	ConnPool *prometheus.GaugeVec
	Request  *prometheus.CounterVec
	Latency  *prometheus.HistogramVec
	Intent   *prometheus.CounterVec
}

var (
	labels        = []string{"app", "op"}
	latencyBucket = []float64{
		4, 8, 16, 32, 64, 128, 256, 512, 1024, 2 * 1024, 4 * 1024, 8 * 1024, 16 * 1024}
	connPoolUpdateInterval = 15 * time.Second
)

func newMetricSet(appName string) *metricSet {
	return &metricSet{
		AppName: appName,
		ConnPool: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: fmt.Sprintf("wpgx_conn_pool"),
				Help: "connection pool status",
			}, labels),
		Request: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: fmt.Sprintf("wpgx_request_total"),
				Help: "how many CRUD operations sent to DB.",
			}, labels),
		Latency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    fmt.Sprintf("wpgx_latency_milliseconds"),
				Help:    "CRUD latency in milliseconds",
				Buckets: latencyBucket,
			}, labels),
		Intent: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: fmt.Sprintf("wpgx_intent_total"),
				Help: "how many intent queries invoked, should be the sum of cached + hit_db.",
			}, labels),
	}
}

func (m *metricSet) Register() {
	err := prometheus.Register(m.ConnPool)
	if err != nil {
		log.Err(err).Msgf("failed to register Prometheus ConnPool gauges")
	}
	err = prometheus.Register(m.Request)
	if err != nil {
		log.Err(err).Msgf("failed to register prometheus Request counters")
	}
	err = prometheus.Register(m.Latency)
	if err != nil {
		log.Err(err).Msgf("failed to register Prometheus Latency histogram")
	}
	err = prometheus.Register(m.Intent)
	if err != nil {
		log.Err(err).Msgf("failed to register Prometheus Intent counters")
	}
}

func (m *metricSet) Unregister() {
	prometheus.Unregister(m.ConnPool)
	prometheus.Unregister(m.Request)
	prometheus.Unregister(m.Latency)
	prometheus.Unregister(m.Intent)
}

func (s *metricSet) MakeObserver(name string, startedAt time.Time) func() {
	return func() {
		if s.Request != nil {
			s.Request.WithLabelValues(s.AppName, name).Inc()
		}
		if s.Latency != nil {
			s.Latency.WithLabelValues(s.AppName, name).Observe(
				float64(time.Since(startedAt).Milliseconds()))
		}
	}
}

func (s *metricSet) CountIntent(name string) {
	if s.Intent != nil {
		s.Intent.WithLabelValues(s.AppName, name).Inc()
	}
}

func (s *metricSet) UpdateConnPoolGauge(stats *pgxpool.Stat) {
	if s.ConnPool != nil {
		s.ConnPool.WithLabelValues(s.AppName, "max_conns").Set(float64(stats.MaxConns()))
		s.ConnPool.WithLabelValues(s.AppName, "total_conns").Set(float64(stats.TotalConns()))
		s.ConnPool.WithLabelValues(s.AppName, "idle_conns").Set(float64(stats.IdleConns()))
		s.ConnPool.WithLabelValues(s.AppName, "acquired_conns").Set(float64(stats.AcquiredConns()))
		s.ConnPool.WithLabelValues(s.AppName, "constructing_conns").Set(
			float64(stats.ConstructingConns()))
	}
}
