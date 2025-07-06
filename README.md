# GFS: A Simplified Google File System with Exactly-Once Record Append Semantics

## Introduction

This project presents a simplified yet robust implementation of the Google File System (GFS) in Python. It serves as a practical tool for understanding the core principles of distributed file systems, including chunk-based storage, fault tolerance through replication, and a centralized master-based architecture. 

A key highlight of this implementation is the enhancement of the `RecordAppend` operation to provide **exactly-once semantics**, a significant improvement over the at-least-once guarantee described in the original GFS paper. This ensures data consistency and integrity, even in the face of network failures and client retries.

## Core Features & Functionalities

*   **Chunk-Based File Storage**: Files are divided into fixed-size chunks (64KB), each with a unique global ID, and replicated across multiple chunk servers.
*   **Master-Chunk Server Architecture**: A single master server manages all filesystem metadata (file namespace, chunk locations, leases), while chunk servers store the actual data.
*   **Fault Tolerance**: Data is replicated across multiple chunk servers (replication factor configurable in `config.py`) to ensure availability in case of server failures. The master monitors chunk servers through regular heartbeats and detects failures.
*   **Exactly-Once Record Append**: A key feature of this implementation is the guarantee of exactly-once semantics for atomic record appends, preventing duplicate data in case of client retries. This is achieved using unique request IDs and duplicate detection at the primary chunk server.

### Record Append Semantics: Exactly-Once vs. At-Least-Once

In distributed systems, especially for operations like appending data, understanding the semantics of operation execution is crucial for data consistency.

*   **At-Least-Once Semantics**: This guarantee means that an operation will be executed one or more times. If a client retries an operation (e.g., due to a network timeout or server crash), the operation might be executed multiple times. For record appends, this can lead to **duplicate data** being written to the file, which is often undesirable and can corrupt data integrity. The original GFS paper's `RecordAppend` operation provided at-least-once semantics, requiring clients to handle potential duplicates.

*   **Exactly-Once Semantics**: This stronger guarantee ensures that an operation is executed precisely once, even if the client retries the request multiple times. For record appends, this means that even if a client retries an append operation, the data will be written to the file only once, preventing duplicates. This is achieved by making the operation **idempotent**. In this GFS implementation, exactly-once semantics for `RecordAppend` are achieved by:
    *   Assigning a unique request ID to each append operation.
    *   The primary chunk server tracking these request IDs and detecting duplicate requests. If a request with an already processed ID is received, it's simply acknowledged without re-executing the append.

**Why Exactly-Once is Better**:
Exactly-once semantics are generally preferred for operations that modify state, such as record appends, because they simplify client logic and ensure data integrity. Clients do not need to implement complex duplicate detection or cleanup mechanisms. It provides a more robust and predictable system behavior, crucial for applications that cannot tolerate data duplication or corruption.
*   **Persistence**: The master and chunk servers persist their state to disk (`gfs_metadata.db`, `gfs_op.log`, and chunk data directories), allowing for recovery after a restart.
*   **Dynamic Lease Management**: The master grants leases to primary chunk replicas for write operations, ensuring consistency and atomicity for mutations.
*   **Client Operations**: Provides a client interface for common file system operations:
    *   `create(filename)`: Creates a new, empty file.
    *   `write(filename, data, offset)`: Writes data to a specific offset within a file. If the offset extends beyond the current file length, new chunks are allocated.
    *   `append(filename, data)`: Atomically appends data to the end of a file, ensuring exactly-once semantics.
    *   `read(filename, offset, length)`: Reads data from a file starting at a given offset for a specified length.
    *   `ls(path)`: Lists files in the specified path (currently only supports listing all files at the root).

## Architecture Overview

The system consists of three main components:

1.  **Master Server (`master_server.py`)**: The brain of the system. It manages all metadata, including the file namespace, chunk locations, and leases. It does not store any file data itself. It handles client requests for metadata and orchestrates chunk operations.
2.  **Chunk Server (`chunk_server.py`)**: The workhorse of the system. It stores file data in fixed-size chunks on its local filesystem and handles read/write requests from clients. It communicates with the master for registration and heartbeats.
3.  **Client (`client.py`)**: The user's gateway to the file system. It provides a simple API for creating, reading, writing, and appending to files. It interacts with the master for metadata and directly with chunk servers for data operations.

## Approach

This GFS implementation follows a simplified, yet functional, approach to demonstrate core distributed file system concepts. Key design decisions include:

*   **Python-based**: Leveraging Python's simplicity and rich ecosystem for rapid prototyping and clear demonstration of concepts.
*   **Flask for APIs**: Using Flask to expose RESTful APIs for communication between clients, master, and chunk servers, simplifying network interactions.
*   **SQLite for Master Metadata**: The master server uses a simple JSON file (`gfs_metadata.db`) to persist its state, simulating a metadata store. For a production system, a more robust database would be used.
*   **Local Filesystem for Chunk Data**: Chunk servers store their data directly on the local filesystem, mimicking how GFS stores chunks on local disks.
*   **Background Threads**: Utilizing Python's threading module for background tasks like heartbeat sending, lease management, and operation queue processing, ensuring non-blocking server operations.
*   **Simplified Consistency Model**: Focuses on primary-replica model for writes with lease mechanisms for consistency. The exactly-once append semantics are a specific enhancement for a critical operation.

## Distributed Systems Principles Applied

This project demonstrates several fundamental distributed systems principles:

*   **Client-Server Architecture**: Clear separation of concerns between clients, master, and chunk servers.
*   **Centralized Control (Master)**: The master acts as a single point of truth for metadata, simplifying consistency management for file system operations. This also highlights the potential for a single point of failure, which is a known characteristic of GFS.
*   **Data Replication**: Chunks are replicated across multiple chunk servers to provide fault tolerance and high availability. If one chunk server fails, data remains accessible from its replicas.
*   **Heartbeating**: Chunk servers periodically send heartbeats to the master, allowing the master to monitor their health and detect failures. This is a common mechanism for liveness detection in distributed systems.
*   **Leasing**: The master grants leases to primary chunk replicas for a limited time, ensuring that only one chunk server can be the primary for a given chunk at any time. This helps maintain consistency during writes and simplifies recovery from failures.
*   **Idempotency**: The implementation of exactly-once record append semantics relies on making the append operation idempotent. By tracking unique request IDs, retried operations do not result in duplicate data, ensuring data integrity.
*   **Consistency vs. Availability (CAP Theorem)**: This GFS implementation leans towards consistency and partition tolerance by having a single master for metadata and using replication for data. While the master is a single point of failure, the system aims for strong consistency for metadata and eventual consistency for data through replication and lease mechanisms.
*   **Fault Tolerance**: The system is designed to tolerate chunk server failures through data replication. The master can detect failed chunk servers and re-replicate data if necessary (though re-replication is not fully implemented in this simplified version).

## How to Run the System

To run the GFS simulation, you can use the provided `run_simulation.sh` script. This script will:

1.  Clean up any previous run's data and logs.
2.  Start the Master Server in the background.
3.  Start two Chunk Servers in the background, each with its own data directory.
4.  Wait for a short period to allow servers to initialize and register.
5.  Execute a client simulation script (`simulate_client.py`) that performs various GFS operations (create, write, append, read, list).
6.  Keep the servers running until you manually stop the script.

**Prerequisites**:
*   Python 3.x installed.
*   `requests` and `Flask` Python libraries installed (`pip install requests Flask`).
*   A Unix-like environment (e.g., Git Bash on Windows, Linux, macOS) to run the `.sh` script.

**Steps to Run**:

1.  Open your terminal or Git Bash.
2.  Navigate to the project's root directory:
    ```bash
    cd C:/Users/priet/OneDrive/Documents/GitHub/GoogleFileSystem/
    ```
3.  Make the simulation script executable (if not already):
    ```bash
    chmod +x run_simulation.sh
    ```
4.  Execute the simulation script:
    ```bash
    ./run_simulation.sh
    ```

**Manual Run (for individual component testing)**:

Alternatively, you can start each component manually in separate terminal windows:

1.  **Start the Master Server**:

    ```bash
    python master_server.py
    ```

2.  **Start one or more Chunk Servers** (in separate terminals):

    ```bash
    python chunk_server.py <port> <data_directory>
    ```

    For example:

    ```bash
    python chunk_server.py 50053 chunk_data_1
    python chunk_server.py 50054 chunk_data_2
    ```

3.  **Run the Client** (in a separate terminal):

    ```bash
    python client.py
    ```
    This will launch an interactive client where you can type commands like `create`, `write`, `append`, `read`, `ls`.

## Future Work

This implementation provides a solid foundation for further exploration of distributed file system concepts. Future enhancements could include:

*   **Dynamic Load Balancing**: Implement a more sophisticated load balancing strategy that considers real-time server metrics for chunk placement and read requests.
*   **Chunk Compression**: Add support for chunk-level compression to optimize storage utilization.
*   **Enhanced Fault Tolerance**: Implement automatic recovery mechanisms for failed chunk servers, including re-replication of lost chunks.
*   **Security Enhancements**: Add authentication and authorization to secure the file system.
*   **Garbage Collection**: Implement a more robust garbage collection mechanism for orphaned chunks.
*   **Snapshotting and Checkpointing**: Add functionality for creating snapshots of the file system state and checkpointing the master's metadata.

---

<p align="center">
  <em>This project is for educational purposes and demonstrates the core concepts of the Google File System.</em>
</p>