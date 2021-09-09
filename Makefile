build:
	go vet ./...
	go build -o bin/mssqldatalayer cmd/mssql/main.go

run:
	go run cmd/mssql/main.go

docker:
	docker build . -t datahub-mssqldatalayer

test:
	go vet ./...
	go test ./... -v

integration:
	./ci/integration-test.sh
