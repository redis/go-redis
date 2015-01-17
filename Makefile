all:
	go test ./... -cpu=1,2,4
	go test ./... -short -race
