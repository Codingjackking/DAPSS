import socket
import struct
import threading
import time
import json
import os
from typing import Iterable, List, Tuple, Optional

BROADCAST_PORT = 9000
BROADCAST_INTERVAL = 3.0
MAGIC = b"DAPSS_DISCOVERY_V2"

REGISTRY_PATH = "active_nodes.json"

def _topics_to_list(topics: Iterable[str]) -> List[str]:
    # normalize topics to a sorted list of unique strings (JSON-safe)
    try:
        return sorted({str(t).strip() for t in topics if str(t).strip()})
    except Exception:
        return []


class PeerDiscovery:
    """
    UDP loopback/broadcast-based peer discovery with topic awareness.
    Each node periodically sends a beacon announcing:
      - IP / Port
      - Subscribed topics
    Peers only connect if they share at least one common topic.
    """

    def __init__(self, node):
        self.node = node
        self.running = False

    # ---------------------------------------------------------------
    def start(self):
        if self.running:
            return
        self.running = True
        threading.Thread(target=self._listen_loop, daemon=True).start()
        threading.Thread(target=self._send_loop, daemon=True).start()

        # Ensure we register with topics as a LIST 
        topics = _topics_to_list(getattr(self.node.subscriber, "subscriptions", []))
        register_node(self.node.host, self.node.port, topics)

    def stop(self):
        self.running = False

    # ---------------------------------------------------------------
    def _send_loop(self):
        """Broadcast IP, port, and subscribed topics."""
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        ip = "127.0.0.1".encode("utf-8").ljust(15, b"\x00")
        while self.running:
            try:
                my_topics = _topics_to_list(getattr(self.node.subscriber, "subscriptions", []))
                topics_json = json.dumps(my_topics).encode("utf-8")
                length = struct.pack("!H", len(topics_json))
                payload = MAGIC + ip + struct.pack("!H", self.node.port) + length + topics_json
                s.sendto(payload, ("127.0.0.1", BROADCAST_PORT))
            except Exception as e:
                print(f"[WARN] Discovery broadcast failed: {e}")
            time.sleep(BROADCAST_INTERVAL)
        s.close()

    # ---------------------------------------------------------------
    def _listen_loop(self):
        """Receive topic-aware discovery beacons and filter by shared topics."""
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        s.bind(("127.0.0.1", BROADCAST_PORT))

        while self.running:
            try:
                data, _ = s.recvfrom(1024)
                if not data.startswith(MAGIC):
                    continue

                body = data[len(MAGIC):]
                ip = body[:15].rstrip(b"\x00").decode("utf-8")
                (port,) = struct.unpack("!H", body[15:17])
                (topics_len,) = struct.unpack("!H", body[17:19])
                topics_json = body[19:19 + topics_len].decode("utf-8")

                peer_topics = set(json.loads(topics_json))
                my_topics = set(getattr(self.node.subscriber, "subscriptions", []))

                # skip self
                if (ip, port) == (self.node.host, self.node.port):
                    continue

                # only connect if there is a topic overlap
                if not (peer_topics & my_topics):
                    continue

                peer = (ip, port)
                if peer not in self.node.peers:
                    self.node.peers.append(peer)
                    print(f"[DISCOVERY] Added peer {peer} (shared: {peer_topics & my_topics})")

            except Exception as e:
                print(f"[WARN] Discovery listen error: {e}")

        s.close()


# --------------------------------------------------------------------
# External discovery (for CLI publishing)
# --------------------------------------------------------------------
def discover_nodes(timeout: float = 2.0, topic_filter: Optional[List[str]] = None) -> List[Tuple[str, int]]:
    """
    Discover only active nodes that share topics with the given filter (UDP-based).
    """
    found = set()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.bind(("127.0.0.1", BROADCAST_PORT))
    sock.settimeout(timeout)

    tf = set(topic_filter or [])
    print(f"[DISCOVERY] Listening for topic-matched nodes for {timeout:.1f}s...")

    start = time.time()
    while time.time() - start < timeout:
        try:
            data, _ = sock.recvfrom(1024)
            if not data.startswith(MAGIC):
                continue

            body = data[len(MAGIC):]
            ip = body[:15].rstrip(b"\x00").decode("utf-8")
            (port,) = struct.unpack("!H", body[15:17])
            (topics_len,) = struct.unpack("!H", body[17:19])
            topics_json = body[19:19 + topics_len].decode("utf-8")
            peer_topics = set(json.loads(topics_json))

            if not tf or (peer_topics & tf):
                found.add((ip, port))

        except socket.timeout:
            break
        except Exception as e:
            print(f"[WARN] Discovery error: {e}")
            break

    sock.close()
    return list(found)


# --------------------------------------------------------------------
# Registry helpers (Windows-friendly fallback)
# --------------------------------------------------------------------
def _normalize_nodes(nodes_raw) -> List[dict]:
    """
    Ensure all nodes in list are dicts with JSON-safe topic lists.
    """
    normalized = []
    for n in nodes_raw or []:
        try:
            host = n["host"] if isinstance(n, dict) else n[0]
            port = n["port"] if isinstance(n, dict) else int(n[1])
            topics = n.get("topics", []) if isinstance(n, dict) else []
            normalized.append({"host": host, "port": int(port), "topics": _topics_to_list(topics)})
        except Exception:
            # skip malformed entries
            continue
    return normalized


def _read_registry() -> List[dict]:
    if not os.path.exists(REGISTRY_PATH):
        return []
    try:
        with open(REGISTRY_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return _normalize_nodes(data)
    except Exception:
        return []


def _save_registry(nodes: List[dict]):
    try:
        # de-dup by (host, port)
        dedup = {}
        for n in nodes:
            key = (n["host"], int(n["port"]))
            dedup[key] = {"host": n["host"], "port": int(n["port"]), "topics": _topics_to_list(n.get("topics", []))}
        with open(REGISTRY_PATH, "w", encoding="utf-8") as f:
            json.dump(list(dedup.values()), f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[WARN] Registry write failed: {e}")


def register_node(host: str, port: int, topics: Iterable[str]):
    """Add or refresh this node in registry with topics."""
    nodes = _read_registry()
    topics_list = _topics_to_list(topics)
    # replace or add
    nodes = [n for n in nodes if not (n["host"] == host and int(n["port"]) == int(port))]
    nodes.append({"host": host, "port": int(port), "topics": topics_list})
    _save_registry(nodes)


def unregister_node(host: str, port: int):
    """Remove this node from registry."""
    nodes = _read_registry()
    nodes = [n for n in nodes if not (n["host"] == host and int(n["port"]) == int(port))]
    _save_registry(nodes)


def registry_discover_nodes(topic_filter: Optional[List[str]] = None, timeout: Optional[float] = None) -> List[Tuple[str, int]]:
    """
    Read registry and return only topic-compatible nodes.
    Accepts optional `timeout` for compatibility with older callers (ignored).
    """
    nodes = _read_registry()
    if not nodes:
        return []
    if not topic_filter:
        return [(n["host"], int(n["port"])) for n in nodes]
    tf = set(topic_filter)
    return [(n["host"], int(n["port"])) for n in nodes if set(n.get("topics", [])) & tf]
