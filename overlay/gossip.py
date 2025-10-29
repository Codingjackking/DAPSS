import json
import random
import socket
import struct
import threading
import time
from typing import Tuple, List


class GossipProtocol:
    """
    Encapsulates gossip/epidemic dissemination.
    Node must provide:
        - .peers : list of (host, port)
        - .deliver_message(topic, content)
    """

    def __init__(self, node, fanout: int = 2, gossip_interval: float = 2.0):
        self.node = node
        self.fanout = fanout
        self.gossip_interval = gossip_interval
        self._seen_lock = threading.Lock()
        self._seen: set[str] = set()
        self.running = False

    def start(self) -> None:
        if not self.running:
            self.running = True
            threading.Thread(target=self._gossip_loop, daemon=True).start()

    def stop(self) -> None:
        self.running = False

    def _gossip_loop(self) -> None:
        while self.running:
            time.sleep(self.gossip_interval)

    def gossip_message(self, message_json: str) -> None:
        """Process and disseminate a message if not seen."""
        msg_id = self._extract_message_id(message_json)
        with self._seen_lock:
            if msg_id in self._seen:
                return
            self._seen.add(msg_id)

        try:
            payload = json.loads(message_json)
            topic = payload.get("topic")
            content = payload.get("content")
            self.node.deliver_message(topic, content)
        except Exception as e:
            print(f"[ERROR] Local delivery failed: {e}")

        peers = list(self.node.peers)
        if not peers:
            return
        for peer in random.sample(peers, min(self.fanout, len(peers))):
            threading.Thread(target=self._send_message, args=(peer, message_json), daemon=True).start()

    def _send_message(self, peer: Tuple[str, int], message_json: str) -> None:
        """Length-prefixed send."""
        try:
            payload = message_json.encode("utf-8")
            header = struct.pack("!I", len(payload))
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(1.5)
                s.connect(peer)
                s.sendall(header + payload)
        except Exception as e:
            print(f"[WARN] Gossip send to {peer} failed: {e}")

    def _extract_message_id(self, message_json: str) -> str:
        try:
            msg = json.loads(message_json)
            return f"{msg.get('topic','')}:{msg.get('timestamp','')}:{msg.get('sender','')}"
        except Exception:
            return str(hash(message_json))
