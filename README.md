What it takes to run this project so far.
1. Go should be installed - brew install go
2. protobuf should be installed - brew install protobuf
3. Golang protobuf installed - go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
4. Path setup:
~ export PATH=$PATH:$(go env GOPATH)/bin

Dependencies for go:
1. go get google.golang.org/protobuf
2. 