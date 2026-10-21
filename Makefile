.PHONY: test lint up down logs

test:
	go test -race ./...

lint:
	go vet ./...
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

# Demo stack. `up` runs in the foreground, so Ctrl-C stops it.
up:
	docker compose up --build

down:
	docker compose down

logs:
	docker compose logs -f evaluator pod-1 pod-2 simulator buyers
