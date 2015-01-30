all:
	go test ./...
	go test ./... -cpu=2
	go test ./... -short -race
