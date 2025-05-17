
# Raft_Algorithm

This project is a Python-based implementation of the Raft consensus algorithm, designed to manage a replicated log across distributed systems. It utilizes gRPC for inter-node communication, simulating a cluster of nodes that maintain consistency through leader election, log replication, and fault tolerance.

## Features

- **Leader Election**: Implements randomized election timeouts to select a leader among nodes.
- **Log Replication**: Ensures that log entries are consistently replicated across all nodes.
- **Client Interaction**: Provides a client interface to send commands to the cluster.
- **Persistent Storage**: Maintains logs and node states across restarts.
- **gRPC Communication**: Facilitates efficient and structured communication between nodes.

- ## Getting Started

### Prerequisites

- Python 3.6 or higher
- `grpcio` and `grpcio-tools` packages

Install the required packages:

```bash
pip install grpcio grpcio-tools
```

### Generating gRPC Code

Before running the application, generate the gRPC code from the `.proto` file:

```bash
python3 -m grpc_tools.protoc -I=./Proto --python_out=. --pyi_out=. --grpc_python_out=. ./Proto/raft.proto
```

### Running the Cluster

Start the Raft cluster by executing:

```bash
python3 main.py
```

This will initialize all nodes as defined in `nodes.txt`.

### Interacting with the Cluster

Use the client interface to send commands:

```bash
python3 client.py
```

Follow the prompts to issue commands to the cluster.

## Configuration

- **`nodes.txt`**: Lists the addresses of all nodes in the cluster. Ensure this file is correctly configured before starting the cluster.

## Logging

Each node maintains its own log directory (`logs_node_1`, `logs_node_2`, etc.) to store log entries and state information. These logs are crucial for recovering state after restarts and for debugging purposes.

