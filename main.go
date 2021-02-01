package main

import (
	"bytes"
	"cloud.google.com/go/pubsub"
	"context"
	oc_prometheus "contrib.go.opencensus.io/exporter/prometheus"
	"github.com/eyalfir/logflag"
	"github.com/namsral/flag"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"go.opencensus.io/stats/view"
	"net/http"
	"strconv"
	"time"
)

var (
	consumed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "consumed_messages",
		Help: "The total number of messages consumed from subscription, acked or not",
	})
	acked = promauto.NewCounter(prometheus.CounterOpts{
		Name: "acked_messages",
		Help: "The total number of messages acked",
	})
	nacked = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "nacked_messages",
		Help: "The total number of messages nacked"},
		[]string{"error"})
	targetURL                 string
	sourceSubscriptionProject string
	sourceSubscriptionName    string
	metricsPort               int
	maxOutstandingMessages    int
	numGoroutines             int
	maxIdleConns              int
	maxConnsPerHost           int
	idleConnTimeout           time.Duration
	maxExtension              time.Duration
	maxExtensionPeriod        time.Duration
	synchronous               bool

	httpClient http.Client
)

func handleMessage(ctx context.Context, m *pubsub.Message) {
	consumed.Inc()
	log.Debug("consumed message...")
	req, err := http.NewRequestWithContext(ctx, "POST", targetURL, bytes.NewReader(m.Data))
	if err != nil {
		log.Error("Bad request...", err)
		nacked.With(prometheus.Labels{"error": "cannot_create_request"}).Inc()
		m.Nack()
		return
	}
	for key, value := range m.Attributes {
		req.Header.Add(key, value)
	}
	resp, requestErr := httpClient.Do(req)
	if requestErr != nil {
		log.Error("Cannot complete http request", requestErr)
		nacked.With(prometheus.Labels{"error": "cannot_do_request"}).Inc()
		m.Nack()
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		log.Errorf("Got status %d, nacking", resp.StatusCode)
		nacked.With(prometheus.Labels{"error": "bad_status"}).Inc()
		m.Nack()
		return
	}
	m.Ack()
	log.Debug("acked")
	acked.Inc()
}

func main() {
	flag.StringVar(&targetURL, "target-url", "", "target URL of the messages")
	flag.StringVar(&sourceSubscriptionProject, "subscription-project", "", "GCP project containing the subscription")
	flag.StringVar(&sourceSubscriptionName, "subscription-name", "", "name of the pubsub subscription")
	flag.IntVar(&metricsPort, "metrics-port", 2121, "port number for the metrics endpoint")
	flag.IntVar(&maxOutstandingMessages, "max-outstanding-messages", 1000, "see https://pkg.go.dev/cloud.google.com/go/pubsub#ReceiveSettings")
	flag.IntVar(&numGoroutines, "num-goroutines", 10, "see https://pkg.go.dev/cloud.google.com/go/pubsub#ReceiveSettings")
	flag.IntVar(&maxIdleConns, "max-idle-connections", 100, "see https://github.com/golang/go/issues/16012")
	flag.IntVar(&maxConnsPerHost, "max-conns-per-host", 5, "see https://github.com/golang/go/issues/16012")
	flag.DurationVar(&idleConnTimeout, "idle-conn-timeout", time.Duration(10)*time.Second, "see https://golang.org/pkg/net/http/")
	flag.DurationVar(&maxExtension, "max-extension", time.Duration(60)*time.Minute, "see https://pkg.go.dev/cloud.google.com/go/pubsub")
	flag.DurationVar(&maxExtensionPeriod, "max-extenstion-period", time.Duration(0), "see https://pkg.go.dev/cloud.google.com/go/pubsub")
	flag.BoolVar(&synchronous, "synchronous", false, "see https://pkg.go.dev/cloud.google.com/go/pubsub")
	flag.Parse()
	logflag.Parse()
	flag.VisitAll(func(thisFlag *flag.Flag) {
		log.Info(thisFlag.Name, " = ", thisFlag.Value)
	})
	log.Debug("Logging level set to debug")
	http.DefaultTransport.(*http.Transport).MaxIdleConns = maxIdleConns
	http.DefaultTransport.(*http.Transport).IdleConnTimeout = idleConnTimeout
	http.DefaultTransport.(*http.Transport).MaxConnsPerHost = maxConnsPerHost
	pe, err := oc_prometheus.NewExporter(oc_prometheus.Options{
		Namespace: "pubsub_http_relay",
	})
	if err != nil {
		log.Fatalf("Failed to create the Prometheus exporter: %v", err)
	}
	for _, subView := range pubsub.DefaultSubscribeViews {

		if registerErr := view.Register(subView); registerErr != nil {
			log.Warning("Cannot register view.", registerErr)
		}
	}

	if metricsPort != 0 {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		mux.Handle("/open_census_metrics", pe)
		log.Infof("serving metrics on port %d, url /metrics", metricsPort)
		go http.ListenAndServe(":"+strconv.Itoa(metricsPort), mux)
	}

	ctx := context.Background()
	sourceClient, err := pubsub.NewClient(ctx, sourceSubscriptionProject)
	if err != nil {
		log.Fatal(err)
	}
	sub := sourceClient.Subscription(sourceSubscriptionName)
	sub.ReceiveSettings.MaxOutstandingMessages = maxOutstandingMessages
	sub.ReceiveSettings.NumGoroutines = numGoroutines
	sub.ReceiveSettings.MaxExtension = maxExtension
	sub.ReceiveSettings.MaxExtensionPeriod = maxExtensionPeriod
	sub.ReceiveSettings.Synchronous = synchronous
	err = sub.Receive(ctx, handleMessage)
	if err != nil {
		log.Fatal("Unable to receive messages. ", err)
	}
}
