FROM golang:1.24-alpine AS builder

WORKDIR /src

COPY go.work .
COPY shared/go.mod shared/go.mod
COPY consumer/go.mod consumer/go.mod
COPY shared shared
COPY consumer consumer

WORKDIR /src/consumer
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/consumer ./cmd/main.go

FROM alpine:3.20
WORKDIR /app
COPY --from=builder /app/consumer /app/consumer
COPY --from=builder /usr/local/go/lib/time/zoneinfo.zip /zoneinfo.zip
ENV ZONEINFO=/zoneinfo.zip
EXPOSE 8081
ENTRYPOINT ["/app/consumer"]