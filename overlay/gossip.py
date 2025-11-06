import json
import random
import socket
import struct
import threading
import time
from statistics import mean
from typing import Tuple, List
from utils.metrics import log_adaptive_stats


class GossipProtocol:
    """
    Adaptive, Persistent, and Consistent Gossip Protocol with On-Demand Recovery.
    """

    def __init__(self, node, fanout: int = 2, gossip_interval: float = 2.0):
        self.node = node
        self.fanout = fanout
        self.gossip_interval = gossip_interval
        self._seen_lock = threading.Lock()
        self.seen: set[str] = set()
        self.running = False

        # --- metrics ---
        self.msg_count = 0
        self.latency_samples = []
        self._metrics_lock = threading.Lock()

        # --- persistence ---
        self.log_file = f"log/gossip_log_{node.port}.json"
        self.msg_store: dict[str, str] = {}  # msg_id -> message_json
        self._store_lock = threading.Lock()

        # --- adaptive tuning ---
        self.min_interval = 0.5
        self.max_interval = 5.0
        self.min_fanout = 1
        self.max_fanout = 6
        self.adapt_every = 5.0
        self.reconcile_every = 10.0
        self.recover_done = False

    # ------------------------------------------------------------------
    def start(self) -> None:
        if not self.running:
            self.running = True
            threading.Thread(target=self._adaptive_loop, daemon=True).start()
            threading.Thread(target=self._reconcile_loop, daemon=True).start()
            threading.Thread(target=self._initial_recover, daemon=True).start()

    def stop(self) -> None:
        self.running = False

    # ------------------------------------------------------------------
    def gossip_message(self, message_json: str) -> None:
        """Deliver and gossip message if unseen."""
        msg_id = self._extract_message_id(message_json)
        with self._seen_lock:
            if msg_id in self.seen:
                return
            self.seen.add(msg_id)

        # persist
        with self._store_lock:
            self.msg_store[msg_id] = message_json
            self._append_log(message_json)

        # local delivery
        start = time.time()
        try:
            payload = json.loads(message_json)
            if "control" in payload:
                self._handle_control_message(payload)
                return
            topic = payload.get("topic")
            content = payload.get("content")
            self.node.deliver_message(topic, content)
        except Exception as e:
            print(f"[ERROR] Local delivery failed: {e}")
        end = time.time()

        with self._metrics_lock:
            self.msg_count += 1
            self.latency_samples.append(end - start)

        # forward
        peers = list(self.node.peers or [])
        if not peers:
            return
        fanout = min(self.fanout, len(peers))
        for peer in random.sample(peers, fanout):
            threading.Thread(target=self._send_message, args=(peer, message_json), daemon=True).start()

    # ------------------------------------------------------------------
    def _send_message(self, peer: Tuple[str, int], message_json: str) -> None:
        """Send framed gossip message to a peer."""
        try:
            payload = message_json.encode("utf-8")
            header = struct.pack("!I", len(payload))
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2.0)
            s.connect(peer)
            s.sendall(header + payload)
        except Exception as e:
            print(f"[WARN] Gossip send to {peer} failed: {e}")
        finally:
            s.close()

    # ------------------------------------------------------------------
    def _adaptive_loop(self) -> None:
        """Adjust fanout/interval dynamically."""
        while self.running:
            time.sleep(self.adapt_every)
            with self._metrics_lock:
                peers_n = len(self.node.peers)
                msg_rate = self.msg_count / self.adapt_every
                avg_latency = mean(self.latency_samples) if self.latency_samples else 0.0
                self.msg_count = 0
                self.latency_samples.clear()

            # Adjust fanout & interval
            if peers_n > 10:
                self.fanout = min(self.max_fanout, 3 + peers_n // 10)
            elif peers_n > 3:
                self.fanout = 3
            else:
                self.fanout = 2

            if msg_rate > 20:
                self.gossip_interval = max(self.min_interval, self.gossip_interval * 0.8)
            elif msg_rate < 5:
                self.gossip_interval = min(self.max_interval, self.gossip_interval * 1.2)

            if avg_latency > 1.0:
                self.gossip_interval = min(self.max_interval, self.gossip_interval * 1.3)
            elif 0 < avg_latency < 0.2:
                self.gossip_interval = max(self.min_interval, self.gossip_interval * 0.9)

            log_adaptive_stats(peers_n, msg_rate, avg_latency, self.fanout, self.gossip_interval)

    # ------------------------------------------------------------------
    def _reconcile_loop(self) -> None:
        """Anti-entropy sync between peers."""
        while self.running:
            time.sleep(self.reconcile_every)
            peers = list(self.node.peers or [])
            if not peers or not self.msg_store:
                continue
            print(f"[SYNC] Periodic sync to {len(peers)} peers ({len(self.msg_store)} msgs cached)")
            for peer in peers:
                for msg_json in list(self.msg_store.values()):
                    threading.Thread(target=self._send_message, args=(peer, msg_json), daemon=True).start()

    # ------------------------------------------------------------------
    def _initial_recover(self) -> None:
        """On startup, request gossip log from any active peer."""
        if self.recover_done:
            return
        time.sleep(2.0)  # give discovery a moment
        if not self.node.peers:
            return
        peer = random.choice(self.node.peers)
        control_msg = json.dumps({
            "control": "SYNC_REQUEST",
            "sender": f"{self.node.host}:{self.node.port}"
        })
        print(f"[RECOVER] Requesting gossip sync from {peer}")
        self._send_message(peer, control_msg)
        self.recover_done = True

    # ------------------------------------------------------------------
    def _handle_control_message(self, payload: dict) -> None:
        """Process control messages for sync requests/responses."""
        ctrl = payload.get("control")
        if ctrl == "SYNC_REQUEST":
            sender = payload.get("sender", "")
            host, port = sender.split(":")
            print(f"[SYNC] Received sync request from {sender}, sending {len(self.msg_store)} messages")
            for msg_json in list(self.msg_store.values()):
                threading.Thread(
                    target=self._send_message,
                    args=((host, int(port)), msg_json),
                    daemon=True
                ).start()
        elif ctrl == "SYNC_RESPONSE":
            msgs = payload.get("messages", [])
            print(f"[SYNC] Received sync response: {len(msgs)} messages")
            for m in msgs:
                self.gossip_message(m)

    # ------------------------------------------------------------------
    def _append_log(self, msg_json: str) -> None:
        """Append to local persistent gossip log."""
        try:
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(msg_json + "\n")
        except Exception as e:
            print(f"[WARN] Failed to persist gossip log: {e}")

    # ------------------------------------------------------------------
    def _extract_message_id(self, message_json: str) -> str:
        try:
            msg = json.loads(message_json)
            return f"{msg.get('topic','')}:{msg.get('timestamp','')}:{msg.get('sender','')}"
        except Exception:
            return str(hash(message_json))
