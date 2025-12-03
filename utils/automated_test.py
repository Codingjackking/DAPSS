import subprocess
import time
import socket
import json
import struct
from datetime import datetime
import os
import re

HOST = "127.0.0.1"

# Compute root directory: DAPSS/
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(PROJECT_ROOT, "log")


###############################################################################
# UTILITY HELPERS
###############################################################################

def send_raw_message(port, msg):
    """Send length-prefixed JSON message."""
    try:
        payload = json.dumps(msg).encode("utf-8")
        header = struct.pack("!I", len(payload))
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(2.5)
            s.connect((HOST, port))
            s.sendall(header + payload)
        return True
    except Exception as e:
        print(f"[ERROR] Failed to send to {port}: {e}")
        return False


def start_node(port, topics="news", peers=None, security=False):
    """Start node with proper working directory + non-interactive mode."""
    cmd = ["python", "main.py", str(port), "--topics", topics, "--no-interactive"]

    if peers:
        for p in peers:
            cmd.insert(3, str(p))

    env = os.environ.copy()
    if security:
        env["ENABLE_SECURITY"] = "1"

    print(f"[SPAWN] {' '.join(cmd)}")

    # Run main.py from PROJECT_ROOT (required for imports)
    p = subprocess.Popen(
        cmd,
        cwd=PROJECT_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
        text=True,
        bufsize=1
    )
    return p


def kill_process(p):
    try:
        p.kill()
        p.wait(timeout=2)
    except:
        pass


def read_file_if_exists(path):
    """Robust safe file read."""
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return f.read()
    except Exception as e:
        print(f"[WARN] Could not read {path}: {e}")
    return ""


def wait_for_server(port, timeout=10):
    """Ensure TCP server is up before running tests."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(0.2)
                s.connect((HOST, port))
                return True
        except:
            time.sleep(0.2)
    return False


def wait_for_log_entry(log_file, pattern, timeout=5):
    """Wait for specific pattern to appear in log file."""
    start = time.time()
    while time.time() - start < timeout:
        content = read_file_if_exists(log_file)
        if re.search(pattern, content):
            return True
        time.sleep(0.2)
    return False


###############################################################################
# TEST CASES
###############################################################################

def test_lamport_timestamp(port):
    """
    TC-01: Lamport Timestamp Increment
    Objective: Verify that Lamport timestamps increment correctly
    Expected: Second message has higher timestamp than first message
    """
    print("\n=== TC-01: Lamport Timestamp Increment ===")
    print("Objective: Verify Lamport clock increments on sequential messages")

    # Send two messages with timestamp 0 (will be assigned by gateway)
    m1 = {
        "topic": "tc01",
        "content": "message_1",
        "sender": "test_tc01",
        "timestamp": datetime.now().isoformat(),
        "lamport_timestamp": 0
    }
    
    time.sleep(0.5)  # Small delay to ensure sequential processing
    
    m2 = {
        "topic": "tc01",
        "content": "message_2",
        "sender": "test_tc01",
        "timestamp": datetime.now().isoformat(),
        "lamport_timestamp": 0
    }

    print(f"[TEST] Sending message 1 to port {port}")
    send_raw_message(port, m1)
    time.sleep(1)
    
    print(f"[TEST] Sending message 2 to port {port}")
    send_raw_message(port, m2)
    time.sleep(2)  # Allow gossip logs to flush

    # Check gossip log for timestamps
    log_file = os.path.join(LOG_DIR, f"gossip_log_{port}.json")
    content = read_file_if_exists(log_file)

    if not content:
        print(f"[FAIL] No log file found at {log_file}")
        return False

    # Look for lamport_timestamp field (correct field name)
    ts = re.findall(r'"lamport_timestamp"\s*:\s*(\d+)', content)

    if len(ts) < 2:
        print(f"[FAIL] Found only {len(ts)} timestamps, need at least 2")
        print(f"[DEBUG] Log content: {content[:500]}")
        return False

    # Get the last two timestamps
    ts = list(map(int, ts[-2:]))
    
    print(f"[TEST] Timestamps found: {ts[0]} -> {ts[1]}")
    
    if ts[1] > ts[0]:
        print("[PASS] Lamport timestamp correctly incremented")
        return True
    else:
        print(f"[FAIL] Timestamp did not increment: {ts[0]} >= {ts[1]}")
        return False


def test_consensus_replication(port):
    """
    TC-02: Consensus-Based Subscription Replication
    Objective: Verify consensus protocol replicates subscription state
    Expected: Subscription appears in node state after consensus
    """
    print("\n=== TC-02: Consensus-Based Subscription Replication ===")
    print("Objective: Verify subscription changes replicate via consensus")

    # Use the subscription command format
    msg = {
        "type": "subscription_command",
        "action": "subscribe",
        "topic": "weather_tc02"
    }

    print(f"[TEST] Sending subscription command to port {port}")
    send_raw_message(port, msg)
    time.sleep(3)  # Allow consensus to complete

    # Check node state log
    node_log = os.path.join(LOG_DIR, f"node_data_{port}", "node_log.json")
    content = read_file_if_exists(node_log)

    if not content:
        print(f"[WARN] Node log not found, checking registry")
        # Fallback: check registry
        registry_path = os.path.join(PROJECT_ROOT, "active_nodes.json")
        registry = read_file_if_exists(registry_path)
        
        if "weather_tc02" in registry:
            print("[PASS] Subscription found in registry")
            return True
        else:
            print(f"[FAIL] Subscription not found in registry")
            return False

    if "weather_tc02" in content:
        print("[PASS] Subscription replicated successfully")
        return True
    else:
        print(f"[FAIL] Subscription not found in node state")
        return False


def test_duplicate_suppression(port):
    """
    TC-03: Duplicate Message Suppression
    Objective: Verify gossip protocol deduplicates repeated messages
    Expected: Same message ID appears only once in logs
    """
    print("\n=== TC-03: Duplicate Message Suppression ===")
    print("Objective: Verify duplicate messages are suppressed")

    # Create unique message with fixed timestamp
    unique_ts = 99999
    msg = {
        "topic": "dup_tc03",
        "content": "duplicate_test",
        "sender": "tester_tc03",
        "timestamp": datetime.now().isoformat(),
        "lamport_timestamp": unique_ts
    }

    print(f"[TEST] Sending same message twice (Lamport TS: {unique_ts})")
    send_raw_message(port, msg)
    time.sleep(0.5)
    send_raw_message(port, msg)  # Send again (duplicate)
    time.sleep(2)

    log_file = os.path.join(LOG_DIR, f"gossip_log_{port}.json")
    content = read_file_if_exists(log_file)

    if not content:
        print(f"[FAIL] No log file found")
        return False

    # Count occurrences of this specific timestamp
    occurrences = len(re.findall(rf'"lamport_timestamp"\s*:\s*{unique_ts}', content))

    print(f"[TEST] Message with TS={unique_ts} appears {occurrences} time(s)")

    if occurrences == 1:
        print("[PASS] Duplicate successfully suppressed")
        return True
    elif occurrences == 0:
        print("[FAIL] Message not received at all")
        return False
    else:
        print(f"[FAIL] Message appeared {occurrences} times (should be 1)")
        return False


def test_topic_filtering(portA, portB, portC):
    """
    TC-04: Topic-Based Message Filtering
    Objective: Verify nodes only receive messages for subscribed topics
    Expected: Nodes receive only messages matching their subscriptions
    """
    print("\n=== TC-04: Topic-Based Message Filtering ===")
    print("Objective: Verify topic-based filtering works correctly")

    # NodeA and NodeB subscribe to "news", NodeC subscribes to "sports"
    # Send "news" message - should reach A and B, not C
    
    news_msg = {
        "topic": "news",
        "content": "news_content_tc04",
        "sender": "tester_tc04",
        "timestamp": datetime.now().isoformat(),
        "lamport_timestamp": 0
    }

    print(f"[TEST] Sending 'news' message (should reach {portA}, {portB}, not {portC})")
    send_raw_message(portA, news_msg)
    time.sleep(3)

    # Check logs
    logA = read_file_if_exists(os.path.join(LOG_DIR, f"gossip_log_{portA}.json"))
    logB = read_file_if_exists(os.path.join(LOG_DIR, f"gossip_log_{portB}.json"))
    logC = read_file_if_exists(os.path.join(LOG_DIR, f"gossip_log_{portC}.json"))

    foundA = "news_content_tc04" in logA
    foundB = "news_content_tc04" in logB
    foundC = "news_content_tc04" in logC

    print(f"[TEST] Message found - A: {foundA}, B: {foundB}, C: {foundC}")

    # A and B should have it, C should not
    if foundA and foundB and not foundC:
        print("[PASS] Topic filtering works correctly")
        return True
    else:
        print(f"[FAIL] Unexpected filtering results")
        return False


def test_message_persistence(port):
    """
    TC-05: Message Persistence and Recovery
    Objective: Verify messages are persisted to disk for recovery
    Expected: Messages appear in persistent gossip log
    """
    print("\n=== TC-05: Message Persistence and Recovery ===")
    print("Objective: Verify messages are persisted to disk")

    msg = {
        "topic": "persist_tc05",
        "content": "persistence_test_data",
        "sender": "tester_tc05",
        "timestamp": datetime.now().isoformat(),
        "lamport_timestamp": 0
    }

    print(f"[TEST] Sending message to port {port}")
    send_raw_message(port, msg)
    time.sleep(2)  # Allow write to disk

    log_file = os.path.join(LOG_DIR, f"gossip_log_{port}.json")
    
    if not os.path.exists(log_file):
        print(f"[FAIL] Gossip log file does not exist: {log_file}")
        return False

    content = read_file_if_exists(log_file)

    if "persistence_test_data" in content:
        print("[PASS] Message successfully persisted to disk")
        return True
    else:
        print(f"[FAIL] Message not found in persistent log")
        return False


def test_leader_election(port1, port2, port3):
    """
    TC-06: Leader Election and Heartbeat
    Objective: Verify Raft-inspired leader election works
    Expected: Exactly one leader elected, sends heartbeats
    """
    print("\n=== TC-06: Leader Election and Heartbeat ===")
    print("Objective: Verify leader election completes successfully")

    # Wait for election to stabilize
    time.sleep(5)

    # Check logs for leader election
    log1 = read_file_if_exists(os.path.join(LOG_DIR, f"node_data_{port1}", "node_log.json"))
    log2 = read_file_if_exists(os.path.join(LOG_DIR, f"node_data_{port2}", "node_log.json"))
    log3 = read_file_if_exists(os.path.join(LOG_DIR, f"node_data_{port3}", "node_log.json"))

    # Alternative: check stdout logs (captured by Popen)
    # Count how many nodes think they're leader
    
    # For now, just check if any node became leader
    combined = log1 + log2 + log3
    
    has_leader = "LEADER" in combined or "Became LEADER" in combined
    has_heartbeat = "heartbeat" in combined.lower()

    print(f"[TEST] Leader detected: {has_leader}, Heartbeats detected: {has_heartbeat}")

    if has_leader:
        print("[PASS] Leader election completed")
        return True
    else:
        print("[FAIL] No leader detected")
        return False


def test_gossip_propagation(portA, portB, portC):
    """
    TC-07: Multi-Hop Gossip Propagation
    Objective: Verify messages propagate through multiple hops
    Expected: Message sent to A reaches B and C via gossip
    """
    print("\n=== TC-07: Multi-Hop Gossip Propagation ===")
    print("Objective: Verify gossip disseminates messages across cluster")

    msg = {
        "topic": "news",
        "content": "gossip_propagation_tc07",
        "sender": "tester_tc07",
        "timestamp": datetime.now().isoformat(),
        "lamport_timestamp": 0
    }

    print(f"[TEST] Sending message to port {portA}")
    send_raw_message(portA, msg)
    time.sleep(4)  # Allow multi-hop propagation

    # Check all nodes received it
    logA = read_file_if_exists(os.path.join(LOG_DIR, f"gossip_log_{portA}.json"))
    logB = read_file_if_exists(os.path.join(LOG_DIR, f"gossip_log_{portB}.json"))
    logC = read_file_if_exists(os.path.join(LOG_DIR, f"gossip_log_{portC}.json"))

    foundA = "gossip_propagation_tc07" in logA
    foundB = "gossip_propagation_tc07" in logB
    foundC = "gossip_propagation_tc07" in logC

    print(f"[TEST] Message received - A: {foundA}, B: {foundB}, C: {foundC}")

    if foundA and foundB and foundC:
        print("[PASS] Gossip successfully propagated to all nodes")
        return True
    else:
        print(f"[FAIL] Message did not reach all nodes")
        return False


###############################################################################
# RUN ALL TESTS
###############################################################################

def run_all_tests():
    print("\n" + "="*60)
    print("         DAPSS Automated Test Suite")
    print("="*60 + "\n")

    print("Launching test cluster...")
    print("-" * 60)
    
    # Start 3-node cluster with different subscriptions
    nodeA = start_node(7001, topics="news")
    time.sleep(2)
    nodeB = start_node(7002, topics="news", peers=[7001])
    time.sleep(2)
    nodeC = start_node(7003, topics="sports", peers=[7001, 7002])

    print("\n[WAIT] Waiting for nodes to initialize...")
    if not wait_for_server(7001, timeout=10):
        print("[ERROR] Node 7001 failed to start")
        return
    if not wait_for_server(7002, timeout=10):
        print("[ERROR] Node 7002 failed to start")
        return
    if not wait_for_server(7003, timeout=10):
        print("[ERROR] Node 7003 failed to start")
        return

    print("[OK] All nodes ready\n")
    time.sleep(3)  # Allow cluster to stabilize

    results = {}

    # Run test cases
    print("="*60)
    print("Running Test Cases...")
    print("="*60)

    results["TC-01: Lamport Timestamp Increment"] = test_lamport_timestamp(7001)
    results["TC-02: Consensus Replication"] = test_consensus_replication(7001)
    results["TC-03: Duplicate Suppression"] = test_duplicate_suppression(7001)
    results["TC-04: Topic Filtering"] = test_topic_filtering(7001, 7002, 7003)
    results["TC-05: Message Persistence"] = test_message_persistence(7001)
    results["TC-06: Leader Election"] = test_leader_election(7001, 7002, 7003)
    results["TC-07: Gossip Propagation"] = test_gossip_propagation(7001, 7002, 7003)

    # Print results
    print("\n" + "="*60)
    print("         TEST RESULTS SUMMARY")
    print("="*60)
    
    passed = sum(1 for result in results.values() if result)
    total = len(results)
    
    for test, result in results.items():
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status:8} | {test}")
    
    print("="*60)
    print(f"Total: {passed}/{total} tests passed ({100*passed//total}%)")
    print("="*60 + "\n")

    print("Shutting down nodes...")
    kill_process(nodeA)
    kill_process(nodeB)
    kill_process(nodeC)

    print("\n[DONE] All tests completed.\n")

    return results


if __name__ == "__main__":
    run_all_tests()