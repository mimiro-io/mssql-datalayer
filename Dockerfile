FROM golang:1.20.3 as build_base

# Set the Current Working Directory inside the container
WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum /app/

# Download all dependencies. Dependencies will be cached if the go.mod and go.sum files are not changed
RUN go mod download

##################################################################################################
FROM build_base AS builder
# Copy the source from the current directory to the Working Directory inside the container
COPY internal /app/internal
COPY resources /app/resources
COPY cmd /app/cmd
COPY config.go /app/

# Build the Go app
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o server cmd/mssql/main.go

RUN go test ./... -v

#################################################################################################
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=builder /app/server .
ADD .env .
ADD resources/default-config.json resources/

# Expose port 8080 to the outside world
EXPOSE 8080

CMD ["./server"]
