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