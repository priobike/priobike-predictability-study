FROM golang:alpine as builder

WORKDIR /app

COPY . .
RUN go mod download
RUN go build -o main .

FROM alpine as runner

WORKDIR /app

COPY --from=builder /app/main .

COPY run-prod.sh .

CMD "./run-prod.sh"