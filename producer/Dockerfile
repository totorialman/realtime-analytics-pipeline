FROM golang:1.26.1-alpine AS builder

WORKDIR /src

COPY go.work .
COPY shared/go.mod shared/go.mod
COPY producer/go.mod producer/go.mod
COPY consumer/go.mod consumer/go.mod 

COPY shared shared
COPY producer producer
COPY consumer consumer 

WORKDIR /src/producer
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/producer ./cmd/main.go

FROM alpine:3.20
WORKDIR /app
COPY --from=builder /app/producer /app/producer
COPY --from=builder /usr/local/go/lib/time/zoneinfo.zip /zoneinfo.zip
ENV ZONEINFO=/zoneinfo.zip
EXPOSE 8080
ENTRYPOINT ["/app/producer"]