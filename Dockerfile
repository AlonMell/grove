FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /grove ./cmd/server

FROM alpine:latest
COPY --from=builder /grove /grove
VOLUME /data
WORKDIR /data
EXPOSE 9736
CMD ["/grove"]