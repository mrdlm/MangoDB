# MangoDB 
![MangoDB Logo](./assets/logo_small.jpg)

MangoDB is a durable, high-performance key-value legacy built in Java. Originally inspired by Riak's BitCask storage engine, it uses an append-only log structure for high write throughput. But unlike BitCask, It is designed to leverage all the underlying CPUs of a machine for maximum performance. 

## Getting Started

### Prerequisites

* **Java:** A Java Development Kit (JDK), version 17 or later, is required to build and run the server.
* **Python:** Python 3 and `pip` are needed to run the integration tests.

### Building the Project

```bash
# Make the script executable (only needed once)
chmod +x run.sh

# Build and run the server
./run.sh

# Run the integration tests
pip install -r requirements.txt
pytest
```

## Operations

The key-value legacy supports the following operations. Commands are case-insensitive.

* **PUT**: Stores a key-value pair.
    * **Usage:** `PUT <key> <value>`
    * **Example:** `PUT mykey myvalue`
    * **Response:** `OK` on success. Returns `RESERVED KEYWORD __TOMBSTONE__` if the value is `__TOMBSTONE__`. Returns `INVALID INPUT` if the format is incorrect.

* **GET**: Retrieves the value associated with a given key.
    * **Usage:** `GET <key>`
    * **Example:** `GET mykey`
    * **Response:** The value associated with the key, or `NOT FOUND` if the key does not exist. Returns `INVALID INPUT` if the format is incorrect.

* **DELETE**: Marks a key for deletion (writes a tombstone record).
    * **Usage:** `DELETE <key>`
    * **Example:** `DELETE mykey`
    * **Response:** `OK` on success, or `NOT FOUND` if the key does not exist. Returns `INVALID INPUT` if the format is incorrect.

* **EXISTS**: Checks if a key exists in the legacy (and is not deleted).
    * **Usage:** `EXISTS <key>`
    * **Example:** `EXISTS mykey`
    * **Response:** `true` if the key exists, `false` otherwise. Returns `INVALID INPUT` if the format is incorrect.

* **FLUSH**: Deletes all the keys in the legacy.
    * **Usage:** `FLUSH`
    * **Response:** `OK` on success.

* **STATUS**: Retrieves statistics about the storage engine.
    * **Usage:** `STATUS`
    * **Response:** A multi-line string containing Disk Size, Data File Counts, Key Directory Size, and Time Since Start-up.