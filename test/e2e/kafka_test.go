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

	brokers := startKafkaContainer(t)
	brokerAddrs := strings.Join(brokers, ",")

	t.Run("Produce", func(t *testing.T) {
		t.Parallel()
		f := testutil.NewTestFixture(t)
		f.SetEnv("KAFKA_BROKERS", brokerAddrs)

		out := f.BuildAndRun("kafkaproducer", "-topic=e2e-orders")
		require.Contains(t, out, "produced message")

		span := f.RequireSingleSpan()
		require.Equal(t, "e2e-orders send", span.Name())
		require.Equal(t, ptrace.SpanKindProducer, span.Kind())

		attrs := testutil.Attrs(span)
		require.Equal(t, "kafka", attrs["messaging.system"])
		require.Equal(t, "e2e-orders", attrs["messaging.destination.name"])
	})

	t.Run("Consume", func(t *testing.T) {
		t.Parallel()
		f := testutil.NewTestFixture(t)
		f.SetEnv("KAFKA_BROKERS", brokerAddrs)

		// The consumer seeds a message then reads it back; the instrumented
		// writer injects trace context into message headers which the reader
		// then extracts — exercising context propagation across the two hooks.
		out := f.BuildAndRun("kafkaconsumer", "-topic=e2e-consume")
		require.Contains(t, out, "consumed message")

		span := testutil.RequireSpan(t, f.Traces(),
			func(s ptrace.Span) bool { return s.Kind() == ptrace.SpanKindConsumer },
		)
		require.Equal(t, "e2e-consume receive", span.Name())
		require.NotEqual(t, ptrace.StatusCodeError, span.Status().Code())

		attrs := testutil.Attrs(span)
		require.Equal(t, "kafka", attrs["messaging.system"])
		require.Equal(t, "e2e-consume", attrs["messaging.destination.name"])
	})
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
