FROM golang:1.24-alpine AS builder
WORKDIR /src
COPY . .
# Add AWS SDK deps not yet in go.mod, then build
RUN go get github.com/aws/aws-sdk-go-v2/config@latest \
           github.com/aws/aws-sdk-go-v2/service/bedrockruntime@latest \
    && go mod tidy \
    && CGO_ENABLED=0 GOOS=linux go build -o /otelbackend .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata python3 py3-pip && \
    pip3 install --no-cache-dir --break-system-packages \
        opentelemetry-sdk \
        opentelemetry-exporter-otlp-proto-http \
        pyyaml
WORKDIR /app
COPY --from=builder /otelbackend .
COPY deploy/rca_simulator/ /app/rca_simulator/
COPY deploy/petclinic-topology.yaml /app/petclinic-topology.yaml
ENV DB_DSN=/data/otel.db
EXPOSE 4317 4318 8080
ENTRYPOINT ["/app/otelbackend"]
