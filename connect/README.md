# rabbirmq

## Hardware Overview:
Model Name: MacBook Pro

Chip: Apple M1 Pro

Total Number of Cores: 8 (6 performance and 2 efficiency)

Memory: 16 GB

## benchmark result
Running tool: go test -benchmem -run=none -bench ^BenchmarkPubSub$ -cpu=8 -benchmem -benchtime=10s .

goos: darwin

goarch: arm64

pkg: github.com/slighter12/event-driven-go/connect

BenchmarkPubSub-8   	   16018	    808515 ns/op	    6779 B/op	     177 allocs/op

PASS

ok  	github.com/slighter12/event-driven-go/connect	24.727s