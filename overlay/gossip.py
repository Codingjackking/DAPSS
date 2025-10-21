import json
import random
import socket
import threading
import time
from typing import Tuple, List


class GossipProtocol:
    """
    Encapsulates gossip/epidemic dissemination.
    Node provides:
      - .peers : List[(host, port)]
      - .deliver_message(topic, content) : local delivery hook
    """

    def __init__(self, node, fanout: int = 2, gossip_interval: float = 2.0):
        self.node = node
        self.fanout = fanout
        self.gossip_interval = gossip_interval
        self._seen_lock = threading.Lock()
        self.seen: set[str] = set()   # message ids we've already processed
        self.running = False

    def start(self) -> None:
        """Starts optional background loop (metrics/anti-entropy later)."""
        if not self.running:
            self.running = True
            threading.Thread(target=self._gossip_loop, daemon=True).start()

    def stop(self) -> None:
        self.running = False

    def _gossip_loop(self) -> None:
        while self.running:
            time.sleep(self.gossip_interval)
            # reserved for anti-entropy / metrics / healing rounds

    def gossip_message(self, message_json: str) -> None:
        """Process and disseminate a message if not seen."""
        msg_id = self._extract_message_id(message_json)

        with self._seen_lock:
            if msg_id in self.seen:
                return
            self.seen.add(msg_id)

        # Local delivery: route through Node -> Subscriber if subscribed
        try:
            payload = json.loads(message_json)
            topic = payload.get("topic")
            content = payload.get("content")
            self.node.deliver_message(topic, content)
        except Exception as e:
            print(f"[ERROR] Local delivery failed: {e}")

        # Pick random peers and forward
        peers: List[Tuple[str, int]] = list(self.node.peers or [])
        if not peers:
            return
        fanout = min(self.fanout, len(peers))
        for peer in random.sample(peers, fanout):
            threading.Thread(target=self._send_message, args=(peer, message_json), daemon=True).start()

    def _send_message(self, peer: Tuple[str, int], message_json: str) -> None:
        """Fire-and-forget TCP send."""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1.5)
            s.connect(peer)
            s.sendall(message_json.encode("utf-8"))
        except Exception as e:
            print(f"[WARN] Gossip send to {peer} failed: {e}")
        finally:
            try:
                s.close()
            except Exception:
                pass

    def _extract_message_id(self, message_json: str) -> str:
        """Stable message id used for dedup (topic + timestamp + sender)."""
        try:
            msg = json.loads(message_json)
            return f"{msg.get('topic','')}:{msg.get('timestamp','')}:{msg.get('sender','')}"
        except Exception:
            return str(hash(message_json))
