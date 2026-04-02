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

# Local execution
start: all
	./scripts/start_local.sh

stop:
	./scripts/stop_local.sh

logs:
	./scripts/logs_local.sh

# Orion execution
start-orion: all
	./scripts/start_orion.sh

stop-orion:
	./scripts/stop_orion.sh

logs-orion:
	./scripts/logs_orion.sh

clean-orion:
	./scripts/clean_orion.sh


# messages/dfs.pb.go is not cleaned
# It is committed to git and generated separately via proto/build.sh
clean:
	rm -rf bin/ logs/ pids/
