import os
import json
import base64
import socket
import struct
import threading
import atexit
import time

from typing import List, Tuple, Optional
from overlay.gossip import GossipProtocol
from overlay.discovery import PeerDiscovery, register_node, unregister_node
from feature.message import Message
from feature.timestamp import LamportClock
from feature.agreement import ConsensusNode

MAX_FRAME_BYTES = 10 * 1024 * 1024  # 10 MB cap


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    """Read exactly n bytes from socket, raising if closed early."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("socket closed during read")
        buf.extend(chunk)
    return bytes(buf)


def _recv_framed(sock: socket.socket) -> bytes:
    """Read a 4-byte length-prefixed message."""
    header = _recv_exact(sock, 4)
    if len(header) != 4:
        raise ValueError("incomplete frame header")
    (length,) = struct.unpack("!I", header)
    if not (1 <= length <= MAX_FRAME_BYTES):
        raise ValueError(f"invalid frame length: {length}")
    return _recv_exact(sock, length)


class Node:
    """
    Overlay node:
    - Combines gossip and peer discovery
    - Persists messages for recovery
    - Maintains consistency across the cluster
    """

    def __init__(self, host: str, port: int, peers: Optional[List[Tuple[str, int]]] = None):
        self.host = host
        self.port = port
        self.peers: List[Tuple[str, int]] = peers[:] if peers else []
        self.subscriber = None

        # --- Lamport Clock ---
        self.lamport_clock = LamportClock()

        # --- Agreement Protocol (Raft-inspired Consensus) ---
        self.consensus = ConsensusNode(self)

        # --- Gossip & Discovery ---
        self.gossip = GossipProtocol(self)
        self.discovery = PeerDiscovery(self)

        # --- Persistence paths ---
        self.node_dir = f"log/node_data_{self.port}"
        self.log_file = os.path.join(self.node_dir, "node_log.json")

        os.makedirs(self.node_dir, exist_ok=True)

        # --- Register node in cluster registry ---
        atexit.register(lambda: unregister_node(self.host, self.port))

    # ------------------------------------------------------------------
    def start_server(self):
        """Main TCP server loop."""
        import atexit
        from overlay.discovery import register_node, unregister_node

        # Ensure safe shutdown deregistration
        atexit.register(lambda: unregister_node(self.host, self.port))

        # Wait until subscriber is attached (topics known)
        topics = []
        if self.subscriber:
            topics = list(self.subscriber.subscriptions)
        register_node(self.host, self.port, topics)

        # --- TCP server setup ---
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("", self.port))
        server.listen(32)

        print(f"[LISTENING] Node running on {self.host}:{self.port}")
        self.consensus.start()  # Start agreement protocol
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


    # ------------------------------------------------------------------
    def _handle_connection(self, conn: socket.socket, addr):
        """
        Handle incoming gossip or control messages.
        Auto-detects framed (binary header) or raw JSON.
        """
        try:
            first_byte = conn.recv(1, socket.MSG_PEEK)
            if not first_byte:
                raise ConnectionError("empty connection")

            # --- Case 1: Framed message ---
            if first_byte[0] not in (ord("{"), ord("[")):
                payload = _recv_framed(conn)
                self._process_payload(payload)
                return

            # --- Case 2: Raw JSON message (unframed) ---
            data = self._recv_until_eof(conn)
            if data:
                self._process_payload(data.encode("utf-8"))

        except (ValueError, ConnectionError, OSError) as e:
            print(f"[WARN] Connection from {addr} failed: {e}")
        except Exception as e:
            print(f"[WARN] Receive error from {addr}: {e}")
        finally:
            try:
                conn.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    def _recv_until_eof(self, conn: socket.socket) -> str:
        """Read until socket EOF (used for unframed JSON)."""
        chunks = []
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                break
            chunks.append(chunk)
        return b"".join(chunks).decode("utf-8").strip()

    # ------------------------------------------------------------------
    def _process_payload(self, payload: bytes):
        """Decode and handle a gossip or consensus payload."""
        try:
            data_str = payload.decode("utf-8")
            data = json.loads(data_str)

            # Check if this is a consensus message
            if "type" in data and data["type"] in [
                "VOTE_REQUEST", "VOTE_RESPONSE", "HEARTBEAT",
                "APPEND_STATE", "ACK", "COMMIT",
                "STATE_CHANGE_REQUEST", "REJOIN_REQUEST", "STATE_SYNC"
            ]:
                # Route to consensus protocol
                self.consensus.handle_consensus_message(data)
            else:
                # Route to gossip protocol
                self.gossip.gossip_message(data_str)

        except (json.JSONDecodeError, UnicodeDecodeError):
            # Binary message or malformed JSON - treat as gossip
            encoded = base64.b64encode(payload).decode("ascii")
            msg = Message(topic="binary", content=encoded, sender=f"{self.host}:{self.port}")
            self.gossip.gossip_message(msg.to_json())

    # ------------------------------------------------------------------
    def deliver_message(self, topic: str, content: str):
        """Deliver messages to local subscribers and persist."""
        if self.subscriber and topic in self.subscriber.subscriptions:
            try:
                self.subscriber.receive_message(topic, content)
                self._append_to_log({"topic": topic, "content": content})
            except Exception as e:
                print(f"[ERROR] Subscriber delivery failed: {e}")

    # ------------------------------------------------------------------
    def _append_to_log(self, record: dict):
        """Append message to persistent log."""
        try:
            with open(self.log_file, "a", encoding="utf-8") as f:
                json.dump(record, f)
                f.write("\n")
        except Exception as e:
            print(f"[WARN] Failed to append to log: {e}")

    # ------------------------------------------------------------------
    def _recover_from_disk(self):
        """Replay persistent log to rebuild local state."""
        if not os.path.exists(self.log_file):
            return
        try:
            with open(self.log_file, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        rec = json.loads(line)
                        topic, content = rec.get("topic"), rec.get("content")
                        if topic and self.subscriber and topic in self.subscriber.subscriptions:
                            self.subscriber.receive_message(topic, content)
                    except Exception:
                        continue
            print(f"[RECOVER] Node {self.port} replayed previous messages from log.")
        except Exception as e:
            print(f"[WARN] Failed to recover from log: {e}")

    # ------------------------------------------------------------------
    def _persist_shutdown_state(self):
        """Flush node state to disk before shutdown."""
        try:
            with open(os.path.join(self.node_dir, "shutdown_marker.txt"), "w") as f:
                f.write(time.ctime())
            print(f"[PERSIST] Node {self.port} saved shutdown state.")
        except Exception as e:
            print(f"[WARN] Could not persist shutdown state: {e}")
