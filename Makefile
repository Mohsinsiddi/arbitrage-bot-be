start:
	make build && make run
build:
	go build -o dex-monitor main.go

run:
	./dex-monitor --config=config.json

PHONY := build run start