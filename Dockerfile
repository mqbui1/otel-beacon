FROM golang:1.24-alpine AS builder
WORKDIR /src
# Copy manifests first for better layer caching
COPY go.mod go.sum ./
# Add AWS SDK deps (not yet pinned in go.mod) and download all modules
RUN go get github.com/aws/aws-sdk-go-v2/config@latest \
           github.com/aws/aws-sdk-go-v2/service/bedrockruntime@latest \
    && go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /otelbackend .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /app
COPY --from=builder /otelbackend .
EXPOSE 4317 4318 8080
ENTRYPOINT ["/app/otelbackend"]
