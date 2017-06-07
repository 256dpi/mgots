all: fmt vet lint

fmt:
	go fmt .
	go fmt ./cmd/mgots-generate

vet:
	go vet .
	go vet ./cmd/mgots-generate

lint:
	golint .
	golint ./cmd/mgots-generate

bench:
	mkdir -p ./bench
	gp run "go test -o ./bench/bin -run None -bench CollectionInsert -memprofile ./bench/mem.out -cpuprofile ./bench/cpu.out"
	go tool pprof -pdf -output ./bench/cpu.pdf ./bench/bin ./bench/cpu.out
	go tool pprof -pdf -output ./bench/mem.pdf ./bench/bin ./bench/mem.out