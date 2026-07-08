//go:build e2e

// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package test

import (
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"go.opentelemetry.io/otelc/test/testutil"
)

func TestKafka(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("kafka testcontainer not supported on windows")
	}

	t.Parallel()

	f := testutil.NewTestFixture(t)
	brokers := startKafkaContainer(t)
	f.SetEnv("KAFKA_BROKERS", strings.Join(brokers, ","))

	// Build and run the producer
	f.BuildAndRun("kafkaproducer", "-topic", "e2e-orders")

	// Build and run the consumer
	f.BuildAndRun("kafkaconsumer", "-topic", "e2e-orders")

	// We expect at least one trace from the interaction
	f.RequireTraceCount(1)

	// Verify producer span
	producerSpan := testutil.RequireSpan(t, f.Traces(),
		func(s ptrace.Span) bool { return s.Kind() == ptrace.SpanKindProducer },
	)
	require.Equal(t, "e2e-orders send", producerSpan.Name())

	// Verify consumer span
	consumerSpan := testutil.RequireSpan(t, f.Traces(),
		func(s ptrace.Span) bool { return s.Kind() == ptrace.SpanKindConsumer },
	)
	require.Equal(t, "e2e-orders receive", consumerSpan.Name())

	// Verify trace context was propagated from producer to consumer
	require.Equal(t, producerSpan.TraceID(), consumerSpan.TraceID(), "trace ID should propagate across Kafka headers")
}

func startKafkaContainer(t *testing.T) []string {
	ctx := t.Context()
	kafkaContainer, err := kafka.Run(ctx, "confluentinc/confluent-local:7.5.0")
	testcontainers.CleanupContainer(t, kafkaContainer)
	require.NoError(t, err)

	brokers, err := kafkaContainer.Brokers(ctx)
	require.NoError(t, err)
	return brokers
}
