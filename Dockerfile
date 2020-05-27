FROM golang:latest as builder

ENV GOPATH=$PWD
ENV CGO_ENABLED=0

COPY . .

RUN go build

FROM alpine:latest

COPY --from=builder /go/sda-mapper ./sda-mapper

RUN addgroup -g 1000 lega && adduser -D -u 1000 -G lega lega

USER 1000

ENTRYPOINT [ "/sda-mapper" ]
