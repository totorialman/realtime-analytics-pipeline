FROM golang:1.25.7-alpine AS builder

WORKDIR /src

COPY gw-currency-wallet/go.mod gw-currency-wallet/go.sum ./gw-currency-wallet/
COPY . .

WORKDIR /src/gw-currency-wallet
RUN go mod download

RUN CGO_ENABLED=0 GOOS=linux go build -mod=readonly -o /app/wallet ./cmd/main.go

FROM alpine
COPY --from=builder /app/wallet /app/wallet

WORKDIR /app
COPY --from=builder /usr/local/go/lib/time/zoneinfo.zip /
ENV TZ="Europe/Moscow"
ENV ZONEINFO=/zoneinfo.zip
EXPOSE 8080
ENTRYPOINT ["/app/wallet"]