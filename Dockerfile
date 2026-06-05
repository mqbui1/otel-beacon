FROM golang:1.24-alpine AS builder
WORKDIR /src
COPY . .
RUN go get github.com/aws/aws-sdk-go-v2/config@latest github.com/aws/aws-sdk-go-v2/service/bedrockruntime@latest && go mod tidy && CGO_ENABLED=0 GOOS=linux go build -o /otelbackend .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=builder /otelbackend .
EXPOSE 4317 4318 8080
ENTRYPOINT ["/app/otelbackend"]
