"""
Lightweight Raft-Inspired Agreement Protocol

Implements consensus for shared state (subscriber lists, topic metadata) across
distributed pub/sub nodes using leader election, heartbeats, and majority voting.

Architecture:
- Control Plane: Metadata changes go through consensus (subscribe/unsubscribe)
- Data Plane: Messages flow through gossip (fast, no consensus needed)
"""

import json
import time
import random
import socket
import struct
import threading
import copy
from typing import Dict, Optional, Tuple, Any


class SharedStateManager:
    """
    Manages shared state that requires consensus across the cluster.

    Tracks:
    - Subscriber mappings (which nodes subscribe to which topics)
    - Topic metadata (active topics, message counts)
    - Cluster membership (active nodes, current leader)
    """

    def __init__(self):
        self.state = {
            "subscribers": {},  # {node_id: {topics: [], lamport_version: int}}
            "topics": {},       # {topic: {subscribers: [], message_count: int, lamport_version: int}}
            "cluster": {
                "nodes": [],
                "leader": None,
                "lamport_version": 0
            }
        }
        self._lock = threading.Lock()

    def create_entry(self, action: str, **kwargs) -> Dict[str, Any]:
        """
        Create a log entry for consensus.

        Args:
            action: Type of state change (subscribe, unsubscribe, add_node, etc.)
            **kwargs: Action-specific data

        Returns:
            Entry dict ready for consensus protocol
        """
        return {
            "action": action,
            "data": kwargs,
            "lamport_ts": None  # Will be set by leader
        }

    def apply_entry(self, entry: Dict[str, Any]) -> None:
        """
        Apply a committed entry to the state.

        Args:
            entry: Committed entry with action, data, and lamport_ts
        """
        with self._lock:
            action = entry["action"]
            data = entry["data"]
            lamport_ts = entry["lamport_ts"]

            if action == "subscribe":
                self._apply_subscribe(data["node_id"], data["topic"], lamport_ts)
            elif action == "unsubscribe":
                self._apply_unsubscribe(data["node_id"], data["topic"], lamport_ts)
            elif action == "add_node":
                self._apply_add_node(data["node_id"], lamport_ts)
            elif action == "remove_node":
                self._apply_remove_node(data["node_id"], lamport_ts)
            elif action == "update_leader":
                self._apply_update_leader(data["leader_id"], lamport_ts)

    def _apply_subscribe(self, node_id: str, topic: str, lamport_ts: int) -> None:
        """Apply subscribe action"""
        # Update subscriber state
        if node_id not in self.state["subscribers"]:
            self.state["subscribers"][node_id] = {"topics": [], "lamport_version": lamport_ts}

        if topic not in self.state["subscribers"][node_id]["topics"]:
            self.state["subscribers"][node_id]["topics"].append(topic)
        self.state["subscribers"][node_id]["lamport_version"] = lamport_ts

        # Update topic metadata
        if topic not in self.state["topics"]:
            self.state["topics"][topic] = {
                "subscribers": [],
                "message_count": 0,
                "lamport_version": lamport_ts
            }

        if node_id not in self.state["topics"][topic]["subscribers"]:
            self.state["topics"][topic]["subscribers"].append(node_id)
        self.state["topics"][topic]["lamport_version"] = lamport_ts

    def _apply_unsubscribe(self, node_id: str, topic: str, lamport_ts: int) -> None:
        """Apply unsubscribe action"""
        if node_id in self.state["subscribers"]:
            if topic in self.state["subscribers"][node_id]["topics"]:
                self.state["subscribers"][node_id]["topics"].remove(topic)
            self.state["subscribers"][node_id]["lamport_version"] = lamport_ts

        if topic in self.state["topics"]:
            if node_id in self.state["topics"][topic]["subscribers"]:
                self.state["topics"][topic]["subscribers"].remove(node_id)
            self.state["topics"][topic]["lamport_version"] = lamport_ts

    def _apply_add_node(self, node_id: str, lamport_ts: int) -> None:
        """Apply add_node action"""
        if node_id not in self.state["cluster"]["nodes"]:
            self.state["cluster"]["nodes"].append(node_id)
        self.state["cluster"]["lamport_version"] = lamport_ts

    def _apply_remove_node(self, node_id: str, lamport_ts: int) -> None:
        """Apply remove_node action"""
        if node_id in self.state["cluster"]["nodes"]:
            self.state["cluster"]["nodes"].remove(node_id)
        self.state["cluster"]["lamport_version"] = lamport_ts

    def _apply_update_leader(self, leader_id: str, lamport_ts: int) -> None:
        """Apply update_leader action"""
        self.state["cluster"]["leader"] = leader_id
        self.state["cluster"]["lamport_version"] = lamport_ts

    def get_state(self) -> Dict[str, Any]:
        """Get a snapshot of current state"""
        with self._lock:
            return copy.deepcopy(self.state)

    def set_state(self, new_state: Dict[str, Any]) -> None:
        """Set state (used during recovery/sync)"""
        with self._lock:
            self.state = copy.deepcopy(new_state)


class ConsensusNode:
    """
    Lightweight Raft-inspired consensus implementation.

    Implements:
    - Leader election with Lamport timestamps as terms
    - Empty heartbeats (keep-alive only)
    - APPEND/ACK/COMMIT state replication
    - Fault tolerance and recovery
    """

    # Node states (Raft)
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"

    def __init__(self, node):
        """
        Initialize consensus node.

        Args:
            node: Reference to the main Node instance
        """
        self.node = node
        self.node_id = f"{node.host}:{node.port}"

        # Raft state
        self.state = self.FOLLOWER
        self.current_term = 0
        self.voted_for = None
        self.leader_id = None

        # Timing 
        self.election_timeout = random.uniform(150, 300) / 1000  # 150-300ms in seconds
        self.heartbeat_interval = 0.05  # 50ms
        self.last_heartbeat = time.time()

        # State management
        self.state_manager = SharedStateManager()

        # Pending entries awaiting ACKs (leader only)
        self.pending_entries = {}  # {lamport_ts: {entry, acks: set, needed: int}}
        self._pending_lock = threading.Lock()

        # Vote tracking (candidate only)
        self.vote_count = 0

        # Partition detection
        self.partition_detected = False
        self.in_minority = False
        self._peer_health_cache = {}  # {peer: last_success_time}
        self._health_check_interval = 2.0  # Check peer health every 2 seconds

        # Running flag
        self.running = False

        # Threads
        self._heartbeat_thread = None
        self._election_thread = None
        self._partition_monitor_thread = None

    def start(self) -> None:
        """Start the consensus protocol"""
        if self.running:
            return

        self.running = True
        print(f"[CONSENSUS] Starting as {self.state}")

        # Start election timeout monitoring
        self._election_thread = threading.Thread(target=self._election_timeout_loop, daemon=True)
        self._election_thread.start()

        # Start partition detection monitoring
        self._partition_monitor_thread = threading.Thread(target=self._partition_monitor_loop, daemon=True)
        self._partition_monitor_thread.start()

        # Register node in cluster
        self._register_self()

    def stop(self) -> None:
        """Stop the consensus protocol"""
        self.running = False
        print("[CONSENSUS] Stopping")

    def _register_self(self) -> None:
        """Register this node in the cluster"""
        entry = self.state_manager.create_entry("add_node", node_id=self.node_id)
  
        if not self.state_manager.state["cluster"]["nodes"]:
            entry["lamport_ts"] = self.node.lamport_clock.tick()
            self.state_manager.apply_entry(entry)

    # ========================================================================
    # Leader Election
    # ========================================================================

    def become_follower(self, term: int) -> None:
        """Transition to FOLLOWER state"""
        self.state = self.FOLLOWER
        self.current_term = term
        self.voted_for = None
        self.last_heartbeat = time.time()
        print(f"[CONSENSUS] Became FOLLOWER (term={term})")

    def become_candidate(self) -> None:
        """Transition to CANDIDATE state and start election"""
        # Check quorum before starting election
        if self.in_minority:
            return

        self.state = self.CANDIDATE
        self.current_term = self.node.lamport_clock.tick()  # Increment term using Lamport clock
        self.voted_for = self.node_id
        self.vote_count = 1  # Vote for self

        print(f"[ELECTION] Starting election for term {self.current_term}")

        # Check if we already have majority 
        total_nodes = len(self.node.peers) + 1
        majority = (total_nodes // 2) + 1

        if self.vote_count >= majority:
            print(f"[ELECTION] Immediate majority ({self.vote_count}/{majority}) - single node")
            self.become_leader()
            return

        # Request votes from all peers
        vote_request = {
            "type": "VOTE_REQUEST",
            "term": self.current_term,
            "candidate_id": self.node_id,
            "lamport_ts": self.current_term
        }

        self._broadcast_consensus_message(vote_request)

    def become_leader(self) -> None:
        """Transition to LEADER state"""
        self.state = self.LEADER
        self.leader_id = self.node_id

        print(f"[CONSENSUS] Became LEADER (term={self.current_term})")

        # Update cluster state with new leader
        entry = self.state_manager.create_entry("update_leader", leader_id=self.node_id)
        entry["lamport_ts"] = self.node.lamport_clock.tick()
        self.state_manager.apply_entry(entry)

        # Start sending heartbeats
        if self._heartbeat_thread is None or not self._heartbeat_thread.is_alive():
            self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
            self._heartbeat_thread.start()

    def on_peer_discovered(self, peer: Tuple[str, int]) -> None:
        """
        Called by discovery when a new peer is found.
        If we're leader but no longer have majority, step down.

        Args:
            peer: (host, port) tuple of newly discovered peer
        """
        if self.state != self.LEADER:
            return

        # Recalculate cluster size
        total_nodes = len(self.node.peers) + 1

        # Count how many nodes we had when we became leader
        # (we had majority which means vote_count >= (old_total // 2) + 1)
        old_total = self.vote_count * 2  

        # If cluster significantly grew, re-elect
        if total_nodes > old_total:
            print(f"[CONSENSUS] Cluster grew ({old_total} → {total_nodes}), stepping down for re-election")
            self.become_follower(self.current_term)

    def _election_timeout_loop(self) -> None:
        """Monitor for leader failure and trigger elections"""
        election_start_time = None

        while self.running:
            if self.state == self.FOLLOWER:
                time_since_heartbeat = time.time() - self.last_heartbeat

                if time_since_heartbeat > self.election_timeout:
                  
                    if not self.in_minority:
                        print(f"[TIMEOUT] Leader timeout ({time_since_heartbeat:.2f}s), starting election")
                    self.become_candidate()
                    election_start_time = time.time()

            elif self.state == self.CANDIDATE:
                #timeout if election fails (split vote)
                if election_start_time is None:
                    election_start_time = time.time()

                time_since_election = time.time() - election_start_time

                if time_since_election > self.election_timeout:
                   
                    if not self.in_minority:
                        print(f"[TIMEOUT] Election timeout ({time_since_election:.2f}s), restarting election")
                    
                    self.election_timeout = random.uniform(150, 300) / 1000
                    self.become_candidate()
                    election_start_time = time.time()

            else:
                # Leader state - reset election tracking
                election_start_time = None

            time.sleep(0.05) 

    # ========================================================================
    # Heartbeat Mechanism (Empty Heartbeats)
    # ========================================================================

    def _heartbeat_loop(self) -> None:
        """Leader sends empty heartbeats every 50ms"""
        heartbeat_count = 0
        while self.running:
            if self.state == self.LEADER:
                heartbeat = {
                    "type": "HEARTBEAT",
                    "term": self.current_term,
                    "leader_id": self.node_id
                    
                }

                self._broadcast_consensus_message(heartbeat)

               
                heartbeat_count += 1
                if heartbeat_count % 20 == 0:
                    print(f"[HEARTBEAT] Sent {heartbeat_count} heartbeats (term={self.current_term})")
            else:
                # No longer leader, stop heartbeat loop
                break

            time.sleep(self.heartbeat_interval)

    # ========================================================================
    # State Replication (APPEND/ACK/COMMIT Model)
    # ========================================================================

    def request_state_change(self, entry: Dict[str, Any], timeout: float = 5.0) -> bool:
        """
        Request a state change (follower to leader or leader processes directly).

        Args:
            entry: State change entry created by SharedStateManager.create_entry()
            timeout: Maximum time to wait for consensus

        Returns:
            True if state change committed, False otherwise
        """
        if self.state == self.LEADER:
           
            return self._append_entry(entry)
        else:
            # Forward request to leader
            if self.leader_id is None:
                print("[CONSENSUS] No leader available")
                return False

            request = {
                "type": "STATE_CHANGE_REQUEST",
                "entry": entry,
                "requester": self.node_id
            }

            # Send to leader
            leader_peer = self._find_peer_by_id(self.leader_id)
            if leader_peer:
                threading.Thread(
                    target=self._send_consensus_message,
                    args=(leader_peer, json.dumps(request)),
                    daemon=True
                ).start()
                print(f"[CONSENSUS] Forwarded state change request to leader {self.leader_id}")
                return True
            else:
                print(f"[CONSENSUS] Could not reach leader {self.leader_id}")
                return False

    def _append_entry(self, entry: Dict[str, Any]) -> bool:
        """
        Leader appends entry and commands followers (APPEND/ACK model).

        Args:
            entry: State change entry

        Returns:
            True if entry will be committed (async), False if failed
        """
        if self.state != self.LEADER:
            return False

        # Check quorum before accepting writes
        if not self._has_quorum():
            print("[PARTITION] Cannot commit entry - lost quorum")
            return False

        # Assign Lamport timestamp (term)
        lamport_ts = self.node.lamport_clock.tick()
        entry["lamport_ts"] = lamport_ts

        # Calculate majority needed
        total_nodes = len(self.node.peers) + 1  
        majority_needed = (total_nodes // 2) + 1

        # Track pending ACKs
        with self._pending_lock:
            self.pending_entries[lamport_ts] = {
                "entry": entry,
                "acks": {self.node_id},  # Leader ACKs immediately
                "needed": majority_needed
            }

        print(f"[LEADER] APPEND entry L:{lamport_ts} action={entry['action']} (need {majority_needed} ACKs)")

        # Tell followers to append 
        append_msg = {
            "type": "APPEND_STATE",
            "term": self.current_term,
            "lamport_ts": lamport_ts,
            "entry": entry
        }

        self._broadcast_consensus_message(append_msg)

        # Check if we immediately have majority (single-node cluster)
        if len(self.pending_entries[lamport_ts]["acks"]) >= majority_needed:
            self._commit_entry(lamport_ts)

        return True

    def _commit_entry(self, lamport_ts: int) -> None:
        """
        Commit an entry after majority ACKs received.

        Args:
            lamport_ts: Lamport timestamp of the entry
        """
        with self._pending_lock:
            if lamport_ts not in self.pending_entries:
                return  # Already committed or unknown

            pending = self.pending_entries[lamport_ts]
            entry = pending["entry"]

        # Apply to state
        self.state_manager.apply_entry(entry)

        print(f"[LEADER] COMMITTED L:{lamport_ts} action={entry['action']}")

        # Broadcast COMMIT to all followers
        commit_msg = {
            "type": "COMMIT",
            "term": self.current_term,
            "lamport_ts": lamport_ts
        }

        self._broadcast_consensus_message(commit_msg)

        # Clean up
        with self._pending_lock:
            del self.pending_entries[lamport_ts]

    # ========================================================================
    # Message Handling
    # ========================================================================

    def handle_consensus_message(self, msg: Dict[str, Any]) -> None:
        """
        Route consensus messages to appropriate handlers.

        Args:
            msg: Consensus protocol message
        """
        msg_type = msg.get("type")

        if msg_type == "VOTE_REQUEST":
            self._handle_vote_request(msg)
        elif msg_type == "VOTE_RESPONSE":
            self._handle_vote_response(msg)
        elif msg_type == "HEARTBEAT":
            self._handle_heartbeat(msg)
        elif msg_type == "APPEND_STATE":
            self._handle_append_state(msg)
        elif msg_type == "ACK":
            self._handle_ack(msg)
        elif msg_type == "COMMIT":
            self._handle_commit(msg)
        elif msg_type == "STATE_CHANGE_REQUEST":
            self._handle_state_change_request(msg)
        elif msg_type == "HEALTH_CHECK":
            #ignore health check logging
            pass
        else:
            print(f"[CONSENSUS] Unknown message type: {msg_type}")

    def _handle_vote_request(self, msg: Dict[str, Any]) -> None:
        """Handle vote request from candidate"""
        candidate_term = msg["term"]
        candidate_id = msg["candidate_id"]

        # Update term if candidate's term is higher
        if candidate_term > self.current_term:
            self.become_follower(candidate_term)

        # Grant vote if haven't voted this term and term is valid
        granted = False
        if candidate_term >= self.current_term and self.voted_for is None:
            self.voted_for = candidate_id
            granted = True
            print(f"[ELECTION] Voted for {candidate_id} (term={candidate_term})")

        # Send vote response
        response = {
            "type": "VOTE_RESPONSE",
            "term": self.current_term,
            "granted": granted,
            "voter_id": self.node_id
        }

        print(f"[ELECTION] Vote {'GRANTED' if granted else 'DENIED'} to {candidate_id}")

        # Send response back to candidate
        candidate_peer = self._find_peer_by_id(candidate_id)
        if candidate_peer:
            threading.Thread(
                target=self._send_consensus_message,
                args=(candidate_peer, json.dumps(response)),
                daemon=True
            ).start()

    def _handle_vote_response(self, msg: Dict[str, Any]) -> None:
        """Handle vote response (candidate only)"""
        if self.state != self.CANDIDATE:
            return

        if msg["granted"]:
            self.vote_count += 1
            total_nodes = len(self.node.peers) + 1
            majority = (total_nodes // 2) + 1

            print(f"[ELECTION] Received vote ({self.vote_count}/{majority})")

            if self.vote_count >= majority:
                self.become_leader()

    def _handle_heartbeat(self, msg: Dict[str, Any]) -> None:
        """Handle heartbeat from leader"""
        leader_term = msg["term"]
        leader_id = msg["leader_id"]

        # If term is higher, become follower
        if leader_term > self.current_term:
            self.become_follower(leader_term)

        # Update leader and reset timeout
        if leader_term >= self.current_term:
            # First heartbeat from this leader
            if self.leader_id != leader_id:
                print(f"[HEARTBEAT] Now following leader {leader_id}")

            self.leader_id = leader_id
            self.last_heartbeat = time.time()

            if self.state != self.FOLLOWER:
                self.become_follower(leader_term)

    def _handle_append_state(self, msg: Dict[str, Any]) -> None:
        """Handle APPEND_STATE command from leader"""
        leader_term = msg["term"]
        lamport_ts = msg["lamport_ts"]
        entry = msg["entry"]

        # Validate term
        if leader_term < self.current_term:
            print(f"[FOLLOWER] Ignoring APPEND from stale leader (term {leader_term} < {self.current_term})")
            return

        if leader_term > self.current_term:
            self.become_follower(leader_term)

        # Store entry as pending (not committed yet)
        with self._pending_lock:
            self.pending_entries[lamport_ts] = entry

        print(f"[FOLLOWER] Logged entry L:{lamport_ts} action={entry['action']}")

        # Send ACK to leader
        ack = {
            "type": "ACK",
            "term": self.current_term,
            "lamport_ts": lamport_ts,
            "node_id": self.node_id
        }

        print(f"[FOLLOWER] Sending ACK for L:{lamport_ts}")

        # Send ACK back to leader
        leader_peer = self._find_peer_by_id(self.leader_id)
        if leader_peer:
            threading.Thread(
                target=self._send_consensus_message,
                args=(leader_peer, json.dumps(ack)),
                daemon=True
            ).start()

    def _handle_ack(self, msg: Dict[str, Any]) -> None:
        """Handle ACK from follower (leader only)"""
        if self.state != self.LEADER:
            return

        lamport_ts = msg["lamport_ts"]
        node_id = msg["node_id"]

        with self._pending_lock:
            if lamport_ts not in self.pending_entries:
                return  # Already committed or unknown

            pending = self.pending_entries[lamport_ts]
            pending["acks"].add(node_id)

            ack_count = len(pending["acks"])
            needed = pending["needed"]

            print(f"[LEADER] Received ACK from {node_id} for L:{lamport_ts} ({ack_count}/{needed})")

            # Check if majority reached
            if ack_count >= needed:
                self._commit_entry(lamport_ts)

    def _handle_commit(self, msg: Dict[str, Any]) -> None:
        """Handle COMMIT notification from leader"""
        lamport_ts = msg["lamport_ts"]

        with self._pending_lock:
            if lamport_ts in self.pending_entries:
                entry = self.pending_entries[lamport_ts]

                # Apply the entry to state
                self.state_manager.apply_entry(entry)

                print(f"[FOLLOWER] Committed L:{lamport_ts} action={entry['action']}")

                # Clean up
                del self.pending_entries[lamport_ts]

    def _handle_state_change_request(self, msg: Dict[str, Any]) -> None:
        """Handle state change request forwarded from follower (leader only)"""
        if self.state != self.LEADER:
            return

        entry = msg["entry"]
        self._append_entry(entry)

    # ========================================================================
    # Partition Detection
    # ========================================================================

    def _count_reachable_peers(self) -> int:
        """
        Count how many peers are currently reachable.
        Updates health cache for both successful and failed checks.

        Returns:
            Number of reachable peers
        """
        reachable = 0
        for peer in self.node.peers:
            if self._is_peer_alive(peer):
                reachable += 1
                # Mark as reachable (positive timestamp)
                self._peer_health_cache[peer] = time.time()
            else:
                # Mark as unreachable (negative timestamp indicates failure)
                self._peer_health_cache[peer] = -time.time()

        return reachable

    def _is_peer_alive(self, peer: Tuple[str, int], timeout: float = 0.5) -> bool:
        """
        Quick health check to see if a peer is reachable.
        Sends a HEALTH_CHECK message to avoid "empty connection" warnings.

        Args:
            peer: (host, port) tuple
            timeout: Connection timeout in seconds

        Returns:
            True if peer responds, False otherwise
        """
        try:
            # Send a health check message
            health_check = json.dumps({"type": "HEALTH_CHECK"})
            payload = health_check.encode("utf-8")
            header = struct.pack("!I", len(payload))

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(timeout)
                s.connect(peer)
                s.sendall(header + payload)
                return True
        except Exception:
            return False

    def _has_quorum(self) -> bool:
        """
        Check if this node can reach a quorum (majority) of the cluster.

        Returns:
            True if quorum is reachable
        """
        if not self.node.peers:
            return True

        total_nodes = len(self.node.peers) + 1  # peers + self
        majority = (total_nodes // 2) + 1
        reachable = self._count_reachable_peers() + 1  # +1 for self

        return reachable >= majority

    def _partition_monitor_loop(self) -> None:
        """
        Continuously monitor for network partitions.
        Detects when this node loses quorum.
        """
        while self.running:
            time.sleep(self._health_check_interval)

            has_quorum = self._has_quorum()

            # Partition detected: we lost quorum
            if not has_quorum and not self.in_minority:
                total_nodes = len(self.node.peers) + 1
                reachable = self._count_reachable_peers() + 1
                majority = (total_nodes // 2) + 1

                print("[PARTITION] Network partition detected!")
                print(f"[PARTITION] Reachable: {reachable}/{total_nodes} (need {majority} for quorum)")
                print("[PARTITION] Entering minority partition mode (read-only)")

                self.partition_detected = True
                self.in_minority = True

                # If leader, step down
                if self.state == self.LEADER:
                    print("[PARTITION] Stepping down as leader (lost quorum)")
                    self.become_follower(self.current_term)

            # Partition healed: regained quorum
            elif has_quorum and self.in_minority:
                print("[PARTITION] ✓ Partition healed - quorum restored!")
                self.partition_detected = False
                self.in_minority = False

    # ========================================================================
    # Network Communication
    # ========================================================================

    def _find_peer_by_id(self, node_id: str) -> Optional[Tuple[str, int]]:
        """
        Find peer tuple (host, port) by node_id.

        Args:
            node_id: Node ID in format "host:port"

        Returns:
            (host, port) tuple or None if not found
        """
        try:
            host, port_str = node_id.split(":")
            port = int(port_str)

            # Check if peer exists in peer list
            for peer in self.node.peers:
                if peer[0] == host and peer[1] == port:
                    return peer

            # If not in peer list but matches own ID, return None 
            if node_id == self.node_id:
                return None

            # Peer not in list, return the parsed tuple
            return (host, port)
        except Exception:
            return None

    def _broadcast_consensus_message(self, msg: Dict[str, Any]) -> None:
        """
        Broadcast consensus message to all peers.

        Args:
            msg: Message dict to broadcast
        """
        msg_json = json.dumps(msg)

        for peer in self.node.peers:
            threading.Thread(
                target=self._send_consensus_message,
                args=(peer, msg_json),
                daemon=True
            ).start()

    def _send_consensus_message(self, peer: Tuple[str, int], msg_json: str) -> None:
        """
        Send consensus message to a single peer.

        Args:
            peer: (host, port) tuple
            msg_json: JSON-serialized message
        """
        try:
            payload = msg_json.encode("utf-8")
            header = struct.pack("!I", len(payload))

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(1.0)
                s.connect(peer)
                s.sendall(header + payload)
        except Exception:
            # Silently fail (peer might be down)
            pass
