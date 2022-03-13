package provider

import (
	"fmt"
	"log"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"

	"go.opentelemetry.io/otel/exporters/zipkin"
)

func Init(exporter, app, service string) TraceProvider {
	switch exporter {
	case "jaeger":
		jaegerUrl := "http://localhost:14268/api/traces"
		return initJaeger(app, service, jaegerUrl)
	case "zipkin":
		zipkinUrl := "http://localhost:9411/api/v2/spans"
		return initZipkin(app, service, zipkinUrl)
	}
	return &provider{nil}
}

func initJaeger(app, service, url string) TraceProvider {
	exporter, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(url)))
	if err != nil {
		fmt.Println("CANNOT CONNECT TO JAEGER ", err.Error())
		return nil
	}
	tp := tracesdk.NewTracerProvider(
		tracesdk.WithBatcher(exporter),
		tracesdk.WithSampler(tracesdk.TraceIDRatioBased(1)),
		tracesdk.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(app),
			semconv.ServiceNamespaceKey.String(service),
		)),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))

	return &provider{
		tp,
	}
}

func initZipkin(app, service, url string) TraceProvider {
	exporter, err := zipkin.New(url)
	if err != nil {
		log.Fatal(err)
	}

	tp := tracesdk.NewTracerProvider(
		tracesdk.WithSpanProcessor(tracesdk.NewBatchSpanProcessor(exporter)),
		tracesdk.WithSampler(tracesdk.TraceIDRatioBased(1)),
		tracesdk.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(app),
			semconv.ServiceNamespaceKey.String(service),
		)),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))

	return &provider{
		tp,
	}
}
