import socket
import threading
from typing import List, Tuple, Optional

from overlay.gossip import GossipProtocol


class Node:
    """
    Overlay node: networking + linkage to dissemination protocol and local subscriber.
    Does NOT own subscription sets; delegates delivery to attached Subscriber.
    """

    def __init__(self, host: str, port: int, peers: Optional[List[Tuple[str, int]]]):
        self.host = host
        self.port = port
        self.peers: List[Tuple[str, int]] = peers[:] if peers else []
        self.gossip = GossipProtocol(self)
        self.subscriber = None  # set by Subscriber(node)

    def start_server(self) -> None:
        """TCP listener for incoming gossip messages."""
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(16)
        print(f"[LISTENING] Node running on {self.host}:{self.port}")
        self.gossip.start()

        while True:
            conn, addr = server.accept()
            threading.Thread(target=self._handle_connection, args=(conn, addr), daemon=True).start()

    def _handle_connection(self, conn: socket.socket, addr) -> None:
        """Decode and hand off to gossip layer."""
        try:
            data = conn.recv(65536).decode("utf-8")
            if data:
                self.gossip.gossip_message(data)
        except Exception as e:
            print(f"[WARN] Receive error from {addr}: {e}")
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def deliver_message(self, topic: str, content: str) -> None:
        """Local delivery path -> Subscriber if subscribed."""
        if self.subscriber and topic in self.subscriber.subscriptions:
            self.subscriber.receive_message(topic, content)
