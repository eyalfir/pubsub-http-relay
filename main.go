package main

import (
	"bytes"
	"cloud.google.com/go/pubsub"
	"context"
	"github.com/eyalfir/logflag"
	"github.com/namsral/flag"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
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
	nacked = promauto.NewCounter(prometheus.CounterOpts{
		Name: "nacked_messages",
		Help: "The total number of messages nacked",
	})
	targetURL                 string
	sourceSubscriptionProject string
	sourceSubscriptionName    string
	metricsPort               int
	maxOutstandingMessages    int
	numGoroutines             int
	maxIdleConns              int
	idleConnTimeout           int

	httpClient http.Client
)

func handleMessage(ctx context.Context, m *pubsub.Message) {
	consumed.Inc()
	log.Debug("consumed message...")
	req, err := http.NewRequest("POST", targetURL, bytes.NewReader(m.Data))
	if err != nil {
		log.Error("Bad request...", err)
		nacked.Inc()
		m.Nack()
		return
	}
	for key, value := range m.Attributes {
		req.Header.Add(key, value)
	}
	resp, requestErr := httpClient.Do(req)
	if requestErr != nil {
		log.Error("Cannot complete http request", requestErr)
		nacked.Inc()
		m.Nack()
		return
	}
	if resp.StatusCode >= 300 {
		log.Errorf("Got status %d, nacking", resp.StatusCode)
		nacked.Inc()
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
	flag.IntVar(&idleConnTimeout, "idle-conn-timeout", 10, "in seconds. see https://golang.org/pkg/net/http/")
	flag.Parse()
	logflag.Parse()
	log.Debug("Logging level set to debug")
	http.DefaultTransport.(*http.Transport).MaxIdleConns = maxIdleConns
	http.DefaultTransport.(*http.Transport).IdleConnTimeout = time.Duration(idleConnTimeout) * time.Second
	if metricsPort != 0 {
		http.Handle("/metrics", promhttp.Handler())
		log.Infof("serving metrics on port %d, url /metrics", metricsPort)
		go http.ListenAndServe(":"+strconv.Itoa(metricsPort), nil)
	}
	ctx := context.Background()
	sourceClient, err := pubsub.NewClient(ctx, sourceSubscriptionProject)
	if err != nil {
		log.Fatal(err)
	}
	sub := sourceClient.Subscription(sourceSubscriptionName)
	sub.ReceiveSettings.MaxOutstandingMessages = maxOutstandingMessages
	sub.ReceiveSettings.NumGoroutines = numGoroutines
	err = sub.Receive(ctx, handleMessage)
	if err != nil {
		log.Fatal("Unable to receive messages")
	}
}
