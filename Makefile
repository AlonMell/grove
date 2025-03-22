COVER_PROFILE := coverage.out
BENCH_PROFILE := bench.out
MODULE_PATH := github.com/grove

.PHONY: test cover cover-html bench lint clean

test:  ## Run unit tests
	@echo "Running tests..."
	go test -v -cover -coverprofile=$(COVER_PROFILE) ./internal/...

cover: test  ## View coverage report
	@echo "➜ Coverage report:"
	go tool cover -func=$(COVER_PROFILE)

cover-html: test  ## View HTML coverage report
	@echo "\n➜ HTML report:"
	go tool cover -html=$(COVER_PROFILE)

bench:  ## Run benchmarks
	@echo "➜ Running benchmarks..."
	go test -bench=. -benchmem -run=^$$ ./internal/... | tee $(BENCH_PROFILE)

lint:  ## Run linters
	@echo "➜ Running linters..."
	golangci-lint run ./...

clean:  ## Clean generated files
	@echo "➜ Cleaning..."
	rm -f $(COVER_PROFILE) $(BENCH_PROFILE)