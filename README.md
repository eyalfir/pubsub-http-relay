# pubsub-http-relay

## Overview

This docker image is pulling messages from a pubsub subscription and POSTing them to an http endpoint.

## Configuration

The implementation is using (https://github.com/namsral/flag), so all the configuration flags can be set using command line arguments, or environmental variables, or configuration files.

To configure a flag using environmental variable, apply uppercase and replace all dashes (`-`) with underscore (`_`).

| variable | optional | default | description | example
|-|-|-|-|-|
| subscription-project | no || GCP project name ||
| subscription-name | no || name of subscription to subscribe to | my-subsbcription |
| target-url | no || the URL to post messages to | http://localhost:12345 |
| metrics-port | yes | 2121 | port to expose prometheus metrics. set to empty string to skip metrics endpoint | 3434 |
| max-outstanding-messages | yes | 1000 | see https://pkg.go.dev/cloud.google.com/go/pubsub#ReceiveSettings") | 10 |
| num-goroutines | yes | 10 | see https://pkg.go.dev/cloud.google.com/go/pubsub#ReceiveSettings") | 2 |
