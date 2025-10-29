import base64
import socket
import struct
import threading
from typing import List, Tuple, Optional
from overlay.gossip import GossipProtocol
from overlay.discovery import PeerDiscovery, register_node, unregister_node
from feature.message import Message

MAX_FRAME_BYTES = 10 * 1024 * 1024  # 10 MB cap


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("socket closed during read")
        buf.extend(chunk)
    return bytes(buf)


def _recv_framed(sock: socket.socket) -> bytes:
    header = _recv_exact(sock, 4)
    (length,) = struct.unpack("!I", header)
    if length <= 0 or length > MAX_FRAME_BYTES:
        raise ValueError(f"Invalid frame length: {length}")
    return _recv_exact(sock, length)


class Node:
    """Overlay node combining gossip and discovery subsystems."""

    def __init__(self, host: str, port: int, peers: Optional[List[Tuple[str, int]]] = None):
        self.host = host
        self.port = port
        self.peers: List[Tuple[str, int]] = peers[:] if peers else []
        self.gossip = GossipProtocol(self)
        self.discovery = PeerDiscovery(self)
        self.subscriber = None

    def start_server(self):
        """Main TCP server loop."""
        import atexit
        atexit.register(lambda: unregister_node(self.host, self.port))

        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("", self.port))
        server.listen(32)
        print(f"[LISTENING] Node running on {self.host}:{self.port}")
        self.gossip.start()
        self.discovery.start()

        try:
            while True:
                conn, addr = server.accept()
                threading.Thread(target=self._handle_connection, args=(conn, addr), daemon=True).start()
        except KeyboardInterrupt:
            print(f"[SHUTDOWN] Node {self.port} shutting down gracefully.")
            unregister_node(self.host, self.port)
        finally:
            server.close()

    def _handle_connection(self, conn: socket.socket, addr):
        try:
            payload = _recv_framed(conn)
            try:
                data_str = payload.decode("utf-8")
                self.gossip.gossip_message(data_str)
            except UnicodeDecodeError:
                encoded = base64.b64encode(payload).decode("ascii")
                msg = Message(topic="binary", content=encoded, sender=f"{self.host}:{self.port}")
                self.gossip.gossip_message(msg.to_json())
        except Exception as e:
            print(f"[WARN] Receive error from {addr}: {e}")
        finally:
            conn.close()

    def deliver_message(self, topic: str, content: str):
        if self.subscriber and topic in self.subscriber.subscriptions:
            try:
                self.subscriber.receive_message(topic, content)
            except Exception as e:
                print(f"[ERROR] Subscriber delivery failed: {e}")
