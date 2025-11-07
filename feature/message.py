# feature/message.py
import json
import socket
import struct
import threading

from datetime import datetime
from typing import Optional, List, Tuple


class Message:
    """Immutable message DTO with JSON (de)serialization and network send helper."""
    __slots__ = ("topic", "content", "sender", "timestamp", "lamport_timestamp")

    def __init__(self, topic: str, content: str, sender: str, timestamp: Optional[str] = None, lamport_timestamp: Optional[int] = None):
        self.topic = topic
        self.content = content
        self.sender = sender
        self.timestamp = timestamp or datetime.now().isoformat()
        self.lamport_timestamp = lamport_timestamp if lamport_timestamp is not None else 0

    # ------------------------
    # Serialization
    # ------------------------
    def to_json(self) -> str:
        return json.dumps({
            "topic": self.topic,
            "content": self.content,
            "sender": self.sender,
            "timestamp": self.timestamp,
            "lamport_timestamp": self.lamport_timestamp
        }, ensure_ascii=False)

    @staticmethod
    def from_json(data: str) -> "Message":
        obj = json.loads(data)
        return Message(
            topic=obj["topic"],
            content=obj["content"],
            sender=obj["sender"],
            timestamp=obj.get("timestamp"),
            lamport_timestamp=obj.get("lamport_timestamp", 0)
        )

    # ------------------------
    # Network Delivery
    # ------------------------
    @staticmethod
    def send_json(host: str, port: int, msg_json: str, timeout: float = 2.5) -> bool:
        """Send a pre-serialized JSON message to a target node using length-prefixed framing."""
        payload = msg_json.encode("utf-8")
        header = struct.pack("!I", len(payload))
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(timeout)
                s.connect((host, port))
                s.sendall(header + payload)
            print(f"[OK] Sent to {host}:{port}")
            return True
        except Exception as e:
            print(f"[ERROR] {host}:{port} → {e}")
            return False

    @staticmethod
    def broadcast(msg_json: str, nodes: List[Tuple[str, int]], timeout: float = 2.5) -> None:
        """Send a JSON message concurrently to multiple nodes."""
        threads = []
        for host, port in nodes:
            t = threading.Thread(target=Message.send_json, args=(host, port, msg_json, timeout))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
        print(f"[DONE] Broadcasted message to {len(nodes)} node(s).")
