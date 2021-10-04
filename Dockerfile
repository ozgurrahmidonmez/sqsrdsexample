FROM golang:1.17

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY *.go ./

RUN go build command/order3/main.go -o /sqsrdsexample

EXPOSE 8080

CMD [ "/sqsrdsexample" ]
