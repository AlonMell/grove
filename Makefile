COVER_PROFILE := coverage.out
BENCH_PROFILE := bench.out
MODULE_PATH := github.com/grove

.PHONY: all test cover bench clean

all: test bench  ## Run tests and benchmarks (default)

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

clean:  ## Clean generated files
	@echo "➜ Cleaning..."
	rm -f $(COVER_PROFILE) $(BENCH_PROFILE)