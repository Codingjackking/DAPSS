"""
DAPSS Comprehensive Metrics System
Tracks and logs performance, health, and system metrics for distributed pub-sub
"""

import json
import threading
import time
import os
from datetime import datetime
from typing import Dict, List, Optional, Any
from collections import deque
from statistics import mean, median, stdev


class MetricsCollector:
    """
    Comprehensive metrics collection for DAPSS nodes.
    Tracks gossip performance, consensus activity, network health, and system resources.
    """

    def __init__(self, node_id: str, metrics_dir: str = "metrics"):
        self.node_id = node_id
        self.metrics_dir = metrics_dir
        self._lock = threading.Lock()
        
        # Ensure metrics directory exists
        os.makedirs(metrics_dir, exist_ok=True)
        
        # File paths
        self.adaptive_file = os.path.join(metrics_dir, f"adaptive_stats_{node_id}.jsonl")
        self.gossip_file = os.path.join(metrics_dir, f"gossip_metrics_{node_id}.jsonl")
        self.consensus_file = os.path.join(metrics_dir, f"consensus_metrics_{node_id}.jsonl")
        self.network_file = os.path.join(metrics_dir, f"network_health_{node_id}.jsonl")
        self.summary_file = os.path.join(metrics_dir, f"summary_{node_id}.json")
        
        # Gossip metrics
        self.gossip_sent = 0
        self.gossip_received = 0
        self.gossip_duplicates = 0
        self.gossip_failures = 0
        self.message_latencies = deque(maxlen=1000)
        
        # Consensus metrics
        self.votes_requested = 0
        self.votes_granted = 0
        self.heartbeats_sent = 0
        self.heartbeats_received = 0
        self.state_changes = 0
        self.leader_elections = 0
        
        # Network metrics
        self.peer_health: Dict[str, Dict] = {}
        self.connection_attempts = 0
        self.connection_failures = 0
        self.bytes_sent = 0
        self.bytes_received = 0
        
        # Performance metrics
        self.message_rates = deque(maxlen=100)
        self.cpu_samples = deque(maxlen=100)
        self.memory_samples = deque(maxlen=100)
        
        # Timing metrics
        self.start_time = time.time()
        self.last_message_time = time.time()
        
        # Counters for windowed metrics
        self.window_start = time.time()
        self.window_messages = 0
        
        print(f"[METRICS] Initialized for node {node_id}")
        print(f"[METRICS] Output directory: {metrics_dir}/")

    # ========================================================================
    # GOSSIP METRICS
    # ========================================================================
    
    def record_gossip_sent(self, peer: str, message_size: int):
        """Record a gossip message sent to a peer"""
        with self._lock:
            self.gossip_sent += 1
            self.bytes_sent += message_size
            self.window_messages += 1
    
    def record_gossip_received(self, sender: str, message_size: int, latency_ms: Optional[float] = None):
        """Record a gossip message received"""
        with self._lock:
            self.gossip_received += 1
            self.bytes_received += message_size
            self.last_message_time = time.time()
            
            if latency_ms is not None:
                self.message_latencies.append(latency_ms)
    
    def record_gossip_duplicate(self):
        """Record a duplicate message detected"""
        with self._lock:
            self.gossip_duplicates += 1
    
    def record_gossip_failure(self, peer: str, reason: str):
        """Record a gossip send failure"""
        with self._lock:
            self.gossip_failures += 1
            self._append_to_file(self.gossip_file, {
                "timestamp": datetime.now().isoformat(),
                "event": "gossip_failure",
                "peer": peer,
                "reason": reason
            })
    
    # ========================================================================
    # CONSENSUS METRICS
    # ========================================================================
    
    def record_vote_request(self, term: int, candidate: str):
        """Record a vote request sent"""
        with self._lock:
            self.votes_requested += 1
            self._append_to_file(self.consensus_file, {
                "timestamp": datetime.now().isoformat(),
                "event": "vote_request",
                "term": term,
                "candidate": candidate
            })
    
    def record_vote_granted(self, term: int, voter: str):
        """Record a vote granted"""
        with self._lock:
            self.votes_granted += 1
            self._append_to_file(self.consensus_file, {
                "timestamp": datetime.now().isoformat(),
                "event": "vote_granted",
                "term": term,
                "voter": voter
            })
    
    def record_heartbeat(self, is_sender: bool, term: int):
        """Record a heartbeat sent or received"""
        with self._lock:
            if is_sender:
                self.heartbeats_sent += 1
            else:
                self.heartbeats_received += 1
    
    def record_state_change(self, old_state: str, new_state: str, term: int):
        """Record a consensus state change"""
        with self._lock:
            self.state_changes += 1
            if new_state == "LEADER":
                self.leader_elections += 1
            
            self._append_to_file(self.consensus_file, {
                "timestamp": datetime.now().isoformat(),
                "event": "state_change",
                "old_state": old_state,
                "new_state": new_state,
                "term": term
            })
    
    # ========================================================================
    # NETWORK HEALTH METRICS
    # ========================================================================
    
    def record_peer_health(self, peer: str, is_healthy: bool, rtt_ms: Optional[float] = None):
        """Record peer health check result"""
        with self._lock:
            if peer not in self.peer_health:
                self.peer_health[peer] = {
                    "healthy_checks": 0,
                    "failed_checks": 0,
                    "last_rtt_ms": None,
                    "last_check": None
                }
            
            if is_healthy:
                self.peer_health[peer]["healthy_checks"] += 1
                if rtt_ms is not None:
                    self.peer_health[peer]["last_rtt_ms"] = rtt_ms
            else:
                self.peer_health[peer]["failed_checks"] += 1
            
            self.peer_health[peer]["last_check"] = datetime.now().isoformat()
    
    def record_connection_attempt(self, peer: str, success: bool):
        """Record a connection attempt to a peer"""
        with self._lock:
            self.connection_attempts += 1
            if not success:
                self.connection_failures += 1
    
    def record_partition_detected(self, peer: str):
        """Record a network partition detected"""
        with self._lock:
            self._append_to_file(self.network_file, {
                "timestamp": datetime.now().isoformat(),
                "event": "partition_detected",
                "peer": peer
            })
    
    def record_partition_healed(self, peer: str):
        """Record a network partition healed"""
        with self._lock:
            self._append_to_file(self.network_file, {
                "timestamp": datetime.now().isoformat(),
                "event": "partition_healed",
                "peer": peer
            })
    
    # ========================================================================
    # ADAPTIVE GOSSIP METRICS
    # ========================================================================
    
    def log_adaptive_stats(self, peers: int, msg_rate: float, avg_latency: float, 
                          fanout: int, interval: float):
        """Log adaptive gossip tuning statistics"""
        record = {
            "timestamp": datetime.now().isoformat(),
            "peers": peers,
            "msg_rate": round(msg_rate, 3),
            "avg_latency": round(avg_latency, 4),
            "fanout": fanout,
            "interval": round(interval, 3),
        }
        
        with self._lock:
            self.message_rates.append(msg_rate)
            self._append_to_file(self.adaptive_file, record)
    
    # ========================================================================
    # SUMMARY STATISTICS
    # ========================================================================
    
    def get_summary(self) -> Dict[str, Any]:
        """Get comprehensive metrics summary"""
        with self._lock:
            uptime = time.time() - self.start_time
            
            # Calculate rates
            gossip_rate = self.gossip_received / uptime if uptime > 0 else 0
            
            # Calculate latency stats
            latency_stats = {}
            if self.message_latencies:
                sorted_latencies = sorted(self.message_latencies)
                latency_stats = {
                    "mean_ms": round(mean(self.message_latencies), 2),
                    "median_ms": round(median(self.message_latencies), 2),
                    "min_ms": round(min(self.message_latencies), 2),
                    "max_ms": round(max(self.message_latencies), 2),
                    "p95_ms": round(sorted_latencies[int(len(sorted_latencies) * 0.95)], 2),
                    "p99_ms": round(sorted_latencies[int(len(sorted_latencies) * 0.99)], 2),
                    "stdev_ms": round(stdev(self.message_latencies), 2) if len(self.message_latencies) > 1 else 0
                }
            
            # Peer health summary
            healthy_peers = sum(1 for p in self.peer_health.values() 
                              if p["healthy_checks"] > p["failed_checks"])
            
            summary = {
                "node_id": self.node_id,
                "timestamp": datetime.now().isoformat(),
                "uptime_seconds": round(uptime, 2),
                
                "gossip": {
                    "sent": self.gossip_sent,
                    "received": self.gossip_received,
                    "duplicates": self.gossip_duplicates,
                    "failures": self.gossip_failures,
                    "rate_per_sec": round(gossip_rate, 2),
                    "latency": latency_stats
                },
                
                "consensus": {
                    "votes_requested": self.votes_requested,
                    "votes_granted": self.votes_granted,
                    "heartbeats_sent": self.heartbeats_sent,
                    "heartbeats_received": self.heartbeats_received,
                    "state_changes": self.state_changes,
                    "leader_elections": self.leader_elections
                },
                
                "network": {
                    "connection_attempts": self.connection_attempts,
                    "connection_failures": self.connection_failures,
                    "connection_success_rate": round(
                        (self.connection_attempts - self.connection_failures) / self.connection_attempts * 100, 2
                    ) if self.connection_attempts > 0 else 0,
                    "bytes_sent": self.bytes_sent,
                    "bytes_received": self.bytes_received,
                    "total_peers": len(self.peer_health),
                    "healthy_peers": healthy_peers
                },
                
                "performance": {
                    "avg_msg_rate": round(mean(self.message_rates), 2) if self.message_rates else 0,
                    "messages_in_window": self.window_messages
                }
            }
            
            return summary
    
    def write_summary(self):
        """Write current summary to file"""
        summary = self.get_summary()
        try:
            with open(self.summary_file, 'w') as f:
                json.dump(summary, f, indent=2)
        except Exception as e:
            print(f"[WARN] Failed to write summary: {e}")
    
    def print_summary(self):
        """Print formatted summary to console"""
        summary = self.get_summary()
        
        print("\n" + "="*70)
        print(f"METRICS SUMMARY - Node {self.node_id}")
        print("="*70)
        print(f"Uptime: {summary['uptime_seconds']:.2f} seconds")
        print()
        
        print("GOSSIP PROTOCOL:")
        print(f"  Messages Sent:     {summary['gossip']['sent']}")
        print(f"  Messages Received: {summary['gossip']['received']}")
        print(f"  Duplicates:        {summary['gossip']['duplicates']}")
        print(f"  Failures:          {summary['gossip']['failures']}")
        print(f"  Rate:              {summary['gossip']['rate_per_sec']:.2f} msg/sec")
        
        if summary['gossip']['latency']:
            lat = summary['gossip']['latency']
            print(f"  Latency Mean:      {lat['mean_ms']:.2f} ms")
            print(f"  Latency P95:       {lat['p95_ms']:.2f} ms")
            print(f"  Latency P99:       {lat['p99_ms']:.2f} ms")
        print()
        
        print("CONSENSUS PROTOCOL:")
        print(f"  Vote Requests:     {summary['consensus']['votes_requested']}")
        print(f"  Votes Granted:     {summary['consensus']['votes_granted']}")
        print(f"  Heartbeats Sent:   {summary['consensus']['heartbeats_sent']}")
        print(f"  Heartbeats Recv:   {summary['consensus']['heartbeats_received']}")
        print(f"  State Changes:     {summary['consensus']['state_changes']}")
        print(f"  Leader Elections:  {summary['consensus']['leader_elections']}")
        print()
        
        print("NETWORK HEALTH:")
        print(f"  Connection Attempts: {summary['network']['connection_attempts']}")
        print(f"  Success Rate:        {summary['network']['connection_success_rate']:.2f}%")
        print(f"  Bytes Sent:          {summary['network']['bytes_sent']:,}")
        print(f"  Bytes Received:      {summary['network']['bytes_received']:,}")
        print(f"  Healthy Peers:       {summary['network']['healthy_peers']}/{summary['network']['total_peers']}")
        print("="*70 + "\n")
    
    # ========================================================================
    # HELPER METHODS
    # ========================================================================
    
    def _append_to_file(self, filepath: str, record: Dict):
        """Thread-safe append to JSON lines file"""
        try:
            with open(filepath, 'a') as f:
                json.dump(record, f)
                f.write('\n')
        except Exception as e:
            print(f"[WARN] Metrics logging failed: {e}")
    
    def reset_window(self):
        """Reset windowed counters"""
        with self._lock:
            self.window_start = time.time()
            self.window_messages = 0


# ============================================================================
# GLOBAL METRICS INSTANCE (Optional singleton pattern)
# ============================================================================

_GLOBAL_METRICS: Optional[MetricsCollector] = None
_METRICS_LOCK = threading.Lock()


def initialize_metrics(node_id: str, metrics_dir: str = "metrics") -> MetricsCollector:
    """Initialize global metrics collector"""
    global _GLOBAL_METRICS
    with _METRICS_LOCK:
        if _GLOBAL_METRICS is None:
            _GLOBAL_METRICS = MetricsCollector(node_id, metrics_dir)
        return _GLOBAL_METRICS


def get_metrics() -> Optional[MetricsCollector]:
    """Get global metrics collector instance"""
    return _GLOBAL_METRICS


# ============================================================================
# LEGACY COMPATIBILITY FUNCTIONS
# ============================================================================

def log_adaptive_stats(peers: int, msg_rate: float, avg_latency: float, fanout: int, interval: float):
    """
    Legacy function for backward compatibility with existing gossip code.
    Logs to a simple adaptive_stats.jsonl file.
    """
    record = {
        "time": datetime.now().isoformat(timespec="seconds"),
        "peers": peers,
        "msg_rate": round(msg_rate, 3),
        "avg_latency": round(avg_latency, 4),
        "fanout": fanout,
        "interval": round(interval, 3),
    }
    
    try:
        with _METRICS_LOCK:
            with open("adaptive_stats.jsonl", "a") as f:
                json.dump(record, f)
                f.write("\n")
    except Exception as e:
        print(f"[WARN] Metrics logging failed: {e}")
    
    # Also log to global metrics if initialized
    if _GLOBAL_METRICS:
        _GLOBAL_METRICS.log_adaptive_stats(peers, msg_rate, avg_latency, fanout, interval)