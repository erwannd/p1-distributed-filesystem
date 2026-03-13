all: bin/controller bin/storage

messages/dfs.pb.go: proto/dfs.proto
	./proto/build.sh

bin/controller: messages/dfs.pb.go controller/controller.go controller/main.go
	go build -o bin/controller ./controller/

bin/storage: messages/dfs.pb.go storage/storage.go storage/main.go
	go build -o bin/storage ./storage/

clean:
	rm -rf bin/controller bin/storage messages/dfs.pb.go