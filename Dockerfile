FROM golang:alpine3.13 as build
WORKDIR /app

COPY . /app/

RUN go mod tidy
RUN go mod vendor
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o instance main.go data.go gossip.go vote.go

FROM alpine:3.13
WORKDIR /app

COPY --from=build /app/simple-server /app/

# ENV HTTP_PORT=8999
# EXPOSE $HTTP_PORT

EXPOSE 8999
CMD "./simple-server"

