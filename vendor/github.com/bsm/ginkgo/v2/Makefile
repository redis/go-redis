default: test

test:
	go test ./...

tidy:
	go mod tidy

version:
	@cat types/version.go | grep 'VERSION' | cut -d' ' -f4 | jq -r .

update: clean update-perform test

.PHONY: update

update-perform:
	@./update.sh

clean:
	find . -maxdepth 1 -type d -not -path './.*' -not -path '.' -exec rm -rf {} \;
	rm -f *.go go.sum
