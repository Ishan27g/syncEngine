package provider

import (
	"context"
	"log"
	"time"

	tracesdk "go.opentelemetry.io/otel/sdk/trace"
)

type TraceProvider interface {
	Get() *tracesdk.TracerProvider
	Close()
}

type provider struct {
	*tracesdk.TracerProvider
}

func (j *provider) Get() *tracesdk.TracerProvider {
	return j.TracerProvider
}

func (j *provider) Close() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer func(ctx context.Context) {
		ctx, cancel = context.WithTimeout(ctx, time.Second*5)
		defer cancel()
		if err := j.Shutdown(ctx); err != nil {
			log.Fatal(err)
		}
	}(ctx)
}
