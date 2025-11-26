# DAPSS - Distributed Pub-Sub System

A fault-tolerant distributed publish-subscribe messaging system implementing Raft-inspired consensus, gossip-based message propagation, and comprehensive security features.

---

## Table of Contents

1. [Features](#features)
2. [Setup Instructions](#setup-instructions)
3. [Running the System](#running-the-system)
4. [Performance Testing](#performance-testing)
5. [Division of Work](#division-of-work)
6. [Project Structure](#project-structure)

---

## Features

### Core Functionality
- **Distributed Pub-Sub Messaging** - Topic-based publish-subscribe system
- **Raft-Inspired Consensus** - Leader election with term-based voting
- **Gossip Protocol** - Epidemic-style message propagation with adaptive fanout
- **Lamport Timestamps** - Logical clocks for causal message ordering
- **Network Partition Handling** - Automatic detection and recovery with quorum-based safety
- **Security Layer** - Node authentication, message signing (HMAC), and encryption (AES)

### Advanced Features
- **Adaptive Metrics** - Dynamic gossip fanout based on cluster size
- **Message Persistence** - Crash recovery with JSON-based storage
- **Topic-Aware Discovery** - UDP broadcast with topic filtering
- **Dynamic Subscriptions** - Runtime topic subscribe/unsubscribe via consensus
- **Health Monitoring** - Continuous peer health checks for partition detection

---

## Setup Instructions

### Prerequisites

- **Python**: 3.10 or higher
- **Operating System**: Windows, Linux, or macOS
- **Network**: Localhost (127.0.0.1) or local network access

### Step 1: Clone the Repository

```bash
git clone <repository-url>
cd DAPSS
```

### Step 2: Create Virtual Environment

**Windows:**
```bash
python -m venv venv
venv\Scripts\activate
```

**Linux/macOS:**
```bash
python3 -m venv venv
source venv/bin/activate
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

**Dependencies:**
- `cryptography>=41.0.0` - For security features (encryption, signing)

### Step 4: Configure Security (Optional)

Edit `config.json` to customize security settings:

```json
{
  "cluster_secret": "DAPSS_CLUSTER_SECRET",
  "encryption_key": "<key>",
  "enable_encryption": true,
  "enable_message_signing": true,
  "enable_node_auth": true
}
```

**Note:** Security is enabled by default. To disable, set all flags to `false`.

---

## Running the System

### Starting a 3-Node Cluster

**Terminal 1 - Node 1:**
```bash
python main.py 5001
```
When prompted, enter topics to subscribe (e.g., `news,updates`)

Enter topics: `news`
**Terminal 2 - Node 2:**
```bash
python main.py 5002 5001
```
Enter topics: `news`

**Terminal 3 - Node 3:**
```bash
python main.py 5003 5001
```
Enter topics: `news`

### Publishing Messages

**Using CLI Publisher:**
```bash
python cli_publish.py <topic> <message>
```

**Example:**
```bash
python cli_publish.py news "Breaking: Distributed for the win!"
```

The CLI publisher automatically discovers nodes subscribed to the topic and sends the message to a gateway node, which then propagates it through the cluster.

### Dynamic Subscription Management

While a node is running, you can use these commands:

```
subscribe <topic>   - Subscribe to a new topic
unsubscribe <topic> - Unsubscribe from a topic
list                - Show current subscriptions
quit                - Stop the node
```

**Example:**
```
> subscribe weather
[Subscriber@5001] Requesting consensus to subscribe to 'weather'...
[Subscriber@5001] Subscribed to weather (consensus reached)
```

---

## Performance Testing

We provide benchmark scripts to evaluate system performance:

### 1. Latency Test

Measures message delivery latency (time from send to receive).

**Step 1: Start nodes with latency logging:**
```bash
# Terminal 1 (publisher)
python main.py 5001
> latency_test

# Terminal 2
python main.py 5002 5001 --latency-log
> latency_test

# Terminal 3
python main.py 5003 5001 --latency-log
> latency_test

```

**Step 2: Run latency test:**
```bash
python latency_test.py 5001 latency_test 50
```

**Step 3: Analyze results:**
```bash
python analyze_latency.py
```

**Expected Output:**
```
Node 5002:
  Messages Received: 50
  Mean Latency: 17.50 ms
  Median Latency: 16.95 ms
  P95: 37.27 ms
  P99: 45.04 ms
```

---

### 2. Throughput Test

Measures how many messages per second the system can handle.

**Step 1: Start a node:**
```bash
python main.py 5001
> throughput_test
```

**Step 2: Run throughput test:**
```bash
python simple_throughput_test.py 5001 10
```

**Expected Output:**
```
Throughput: 152.30 messages/second
Avg Interval: 6.57 ms/message
```

---

### 3. Reliability Test

Verifies all messages are delivered to all subscribers.

**Step 1: Start 3 nodes:**
```bash
# Terminal 1
python main.py 5001
> reliability_test

# Terminal 2
python main.py 5002 5001
> reliability_test

# Terminal 3
python main.py 5003 5001
> reliability_test
```

**Step 2: Run reliability test:**
```bash
python simple_reliability_test.py 5001 reliability_test 20
```

**Step 3: Verify manually:**
Check all three node terminals - you should see all 20 numbered messages (msg_id 0-19) on each node.

---

## Performance Results

Our system achieves the following performance characteristics:

| Metric | Value | Description |
|--------|-------|-------------|
| **Mean Latency** | 16.89 ms | Average message delivery time |
| **P95 Latency** | 37.27 ms | 95% of messages delivered within this time |
| **P99 Latency** | 45.04 ms | 99% of messages delivered within this time |
| **Throughput** | 150+ msg/s | Messages per second (single publisher) |
| **Partition Detection** | ~2.5 seconds | Time to detect network partition |
| **Partition Recovery** | ~5.5 seconds | Time to recover from partition |

---

## Division of Work

### Naing Htet
- **Initial project structure** - Created overlay network foundation and directory organization
- **Peer discovery** (`overlay/discovery.py`) - UDP broadcast with topic-aware filtering
- **CLI publisher** (`cli_publish.py`) - External message gateway with automatic node discovery
- **Gossip protocol** (`overlay/gossip.py`) - Epidemic message propagation with adaptive metrics and persistence
- **Gossip metrics system** (`utils/metrics.py`, `utils/plot_metrics.py`) - Performance tracking and visualization
- **Latency benchmarks** (`latency_test.py`, `analyze_latency.py`) - Message latency measurement and analysis

### Timothy Phan
- **Lamport timestamps** (`feature/timestamp.py`) - Logical clock for causal message ordering
- **Raft consensus protocol** (`feature/agreement.py`) - Leader election, log replication, and state management
- **Network partition handling** - Quorum-based detection and automatic recovery
- **Security layer** (`feature/security.py`) - Node authentication, message signing, and encryption
- **Performance benchmarks** (`throughput_test.py`, `reliability_test.py`) - Throughput and reliability testing

---

## Project Structure

```
DAPSS/
├── main.py                          # Node entry point with CLI interface
├── cli_publish.py                   # External message publisher
├── config.json                      # Security configuration
├── requirements.txt                 # Python dependencies
│
├── overlay/                         # Network layer
│   ├── node.py                      # TCP overlay network & message routing
│   ├── gossip.py                    # Gossip protocol with adaptive metrics
│   └── discovery.py                 # UDP peer discovery & registry
│
├── feature/                         # Core distributed systems features
│   ├── agreement.py                 # Raft consensus + partition handling
│   ├── security.py                  # Authentication, signing, encryption
│   ├── timestamp.py                 # Lamport logical clock
│   └── message.py                   # Message data structure
│
├── subscriber/                      # Application layer
│   └── subscriber.py                # Dynamic subscription management
│
├── publisher/                       # Publisher components
│   └── publisher.py                 # Publisher logic
│
├── utils/                           # Utilities
│   ├── metrics.py                   # Performance metrics tracking
│   └── plot_metrics.py              # Metrics visualization
│
└── (Performance Tests)              # Benchmark scripts
    ├── latency_test.py              # Latency measurement
    ├── analyze_latency.py           # Latency analysis
    ├── throughput_test.py           # Throughput measurement
    └── reliability_test.py          # Reliability verification
```
---

## Troubleshooting

### Issue: "Connection refused"
**Solution:** Ensure nodes are started in order (bootstrap node first)

### Issue: "No active nodes detected"
**Solution:** Wait 2-3 seconds after starting nodes before publishing

### Issue: Messages not appearing on all nodes
**Solution:** Verify all nodes are subscribed to the same topic

### Issue: "Address already in use"
**Solution:** Kill any existing processes on the port:
```bash
# Windows
netstat -ano | findstr :5001
taskkill /PID <pid> /F

# Linux
lsof -ti:5001 | xargs kill -9
```

---

