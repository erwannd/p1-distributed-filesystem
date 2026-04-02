# How to test
- Steps to test logging using scripts 
    ```bash
    # Start Controller & Storage Nodes
    ./scripts/start.sh

    # If you want to log to the terminal use
    ./scripts/logs.sh

    # Stop the application
    ./scripts/stop.sh
    ```

- Client
    ```bash
    # Store file
    ./bin/client --controller <host:port> store --file <filepath>

    # Retrieve file
    ./bin/client --controller <host:port> retrieve --file <filename> --output <output_dir>

    # List file
    # TBD
    ```

# Design Flow

## Corruption Detection
<img src="./diagrams/corruption-detection-uml.png">

1. Client sends `RetrieveRequest` to the Controller with the filename.
2. Controller sends a list of chunks + nodes that have a copy of each chunk (`RetrieveResponse`)
3. Client fetches chunks in parallel. For each chunk, it sends `RetrieveChunkRequest` to one storage node. The request includes:
    - `ChunkInfo` (`filename`, `chunk_index`)
    - `replica_hints`, which are the other nodes known to hold replicas of that chunk
4. The contacted storage node verifies its local chunk against the checksum sidecar stored on disk.
    - If the local copy is valid, it returns the chunk immediately.
    - If the local copy is corrupted, it tries to repair using the nodes in `replica_hints`.
    - If those hints fail, for example because nodes are down or the list is stale, it sends `ChunkLocationsRequest` to the Controller to get a refreshed list of holders for that chunk.
    - The storage node then sends `RepairChunkRequest` to candidate replicas one at a time.
    - A replica that receives `RepairChunkRequest` only checks its own local chunk against its own local checksum.
    - If valid, it returns the chunk in `RepairChunkResponse`.
    - If invalid or missing, it returns failure immediately.
    - Importantly, a replica does not start another repair attempt from inside `RepairChunkRequest`, because that would create repair chains or loops, make latency unpredictable, and allow one client read to trigger a cascade of cross-node recovery work.
5. Once the original storage node gets a valid chunk from a replica, it overwrites its corrupted local copy and recomputes the checksum sidecar.
6. The storage node then returns the repaired chunk to the client.


# Testing on Orion

## Corruption Detection
Corruption test file is located on `orion01:/bigdata/students/aawihardja/datasets/corruption.txt`

```bash
# SSH to orion01

# Cleanup
make clean-orion

# Stop stale process
make stop-orion

make start-orion

# In a second terminal ssh to orion01 and view the logs
make logs-orion

# In a third terminal, ssh to orion01, start the client, and store the test file
./bin/client --config config.orion.json --file /bigdata/students/aawihardja/datasets/corruption.txt --chunk-size 1048576

# Find primary replica from controller snapshot
cat /bigdata/students/aawihardja/controller_snapshot.json

# In a fourth terminal, ssh to the replica (e.g. orion06), and change the file content w/o changing the checksum
printf 'hahahaha\n' > /bigdata/students/storage/$PRIMARY_HOST/node_${PORT}/corruption.txt_chunk_0

# In the client terminal, retrieve the file (this will trigger recovery)
./bin/client --config config.orion.json retrieve --file corruption.txt

# Delete the file from the dfs
./bin/client --config config.orion.json delete --file corruption.txt

# Delete retrieved file
rm /bigdata/students/aawihardja/downloads/corruption.txt
```