FROM golang:1.17 AS build

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY command/order3/main.go ./command/order3/main.go
COPY orders/orders.go ./orders/orders.go

# RUN CGO_ENABLED=0 GO111MODULE=off GOOS=linux go install ./command/order3/main.go
RUN go install ./command/order3/main.go

FROM gcr.io/distroless/base-debian10

WORKDIR /

COPY --from=build /go/bin/main /go/bin/main

EXPOSE 8991
USER root::root

ENTRYPOINT ["/go/bin/main"]

