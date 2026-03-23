UTILS = $(wildcard utils/*.go)

all: bin/controller bin/storage bin/client

bin/:
	mkdir -p bin/

bin/controller: $(wildcard controller/*.go) $(UTILS)
	go build -o bin/controller ./controller/

bin/storage: $(wildcard storage/*.go) $(UTILS)
	go build -o bin/storage ./storage/

bin/client: $(wildcard client/*.go) $(UTILS)
	go build -o bin/client ./client/

start: all
	./scripts/start.sh

stop:
	./scripts/stop.sh

logs:
	./scripts/logs.sh


# Note: messages/dfs.pb.go is intentionally not cleaned
# It is committed to git and generated separately via proto/build.sh
clean:
	rm -rf bin/ logs/ pids/