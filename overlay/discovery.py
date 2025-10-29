import socket
import struct
import threading
import time
import json
import os

BROADCAST_PORT = 9000
BROADCAST_INTERVAL = 3.0
MAGIC = b"DAPSS_DISCOVERY"

REGISTRY_PATH = "active_nodes.json"


class PeerDiscovery:
    """
    UDP loopback/broadcast-based peer discovery.
    Each node periodically sends a beacon announcing its listening TCP port.
    Also writes itself into a small JSON registry for Windows/local testing.
    """

    def __init__(self, node):
        self.node = node
        self.running = False

    def start(self):
        if self.running:
            return
        self.running = True
        # start both threads
        threading.Thread(target=self._listen_loop, daemon=True).start()
        threading.Thread(target=self._send_loop, daemon=True).start()
        # always register node locally for CLI discovery
        register_node(self.node.host, self.node.port)

    def stop(self):
        self.running = False

    # ---- internal ----
    def _send_loop(self):
        """Broadcast this node's port and IP (loopback-safe for local demos)."""
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        ip = "127.0.0.1".encode("utf-8").ljust(15, b"\x00")
        payload = MAGIC + ip + struct.pack("!H", self.node.port)
        while self.running:
            try:
                s.sendto(payload, ("127.0.0.1", BROADCAST_PORT))
                # Debug: print(f"[DISCOVERY] Beacon sent from {self.node.port}")
            except Exception as e:
                print(f"[WARN] Discovery broadcast failed: {e}")
            time.sleep(BROADCAST_INTERVAL)
        s.close()

    def _listen_loop(self):
        """Receive discovery beacons and add new peers."""
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        s.bind(("127.0.0.1", BROADCAST_PORT))
        while self.running:
            try:
                data, _ = s.recvfrom(64)
                if not data.startswith(MAGIC):
                    continue
                body = data[len(MAGIC):]
                ip = body[:15].rstrip(b"\x00").decode("utf-8")
                (port,) = struct.unpack("!H", body[15:])
                if (ip, port) != (self.node.host, self.node.port):
                    peer = (ip, port)
                    if peer not in self.node.peers:
                        self.node.peers.append(peer)
                        print(f"[DISCOVERY] Added new peer {peer}")
            except Exception as e:
                print(f"[WARN] Discovery listen error: {e}")
        s.close()


# --------------------------------------------------------------------
# External discovery for CLI / tools
# --------------------------------------------------------------------
def discover_nodes(timeout: float = 2.0):
    """Listen for discovery beacons and return a list of active (host, port) nodes."""
    found = set()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.bind(("127.0.0.1", BROADCAST_PORT))
    sock.settimeout(timeout)

    start = time.time()
    print(f"[DISCOVERY] Listening for active nodes for {timeout:.1f}s...")

    while time.time() - start < timeout:
        try:
            data, _ = sock.recvfrom(64)
            if not data.startswith(MAGIC):
                continue
            body = data[len(MAGIC):]
            ip = body[:15].rstrip(b"\x00").decode("utf-8")
            (port,) = struct.unpack("!H", body[15:])
            found.add((ip, port))
        except socket.timeout:
            break
        except Exception as e:
            print(f"[WARN] Discovery error: {e}")
            break

    sock.close()
    return list(found)


# --------------------------------------------------------------------
# Registry-based fallback discovery (for Windows loopback)
# --------------------------------------------------------------------
def _save_registry(nodes):
    """Internal helper to write registry safely."""
    try:
        unique_nodes = list({(h, int(p)) for h, p in nodes})
        with open(REGISTRY_PATH, "w") as f:
            json.dump(unique_nodes, f)
    except Exception as e:
        print(f"[WARN] Registry write failed: {e}")

def register_node(host: str, port: int):
    """Add or refresh this node in the registry."""
    nodes = []
    try:
        if os.path.exists(REGISTRY_PATH):
            with open(REGISTRY_PATH, "r") as f:
                nodes = json.load(f)
    except Exception:
        pass
    if (host, port) not in nodes:
        nodes.append((host, port))
    _save_registry(nodes)

def unregister_node(host: str, port: int):
    """Remove this node from the registry on shutdown."""
    if not os.path.exists(REGISTRY_PATH):
        return
    try:
        with open(REGISTRY_PATH, "r") as f:
            nodes = json.load(f)
        nodes = [(h, int(p)) for h, p in nodes if (h, int(p)) != (host, port)]
        _save_registry(nodes)
    except Exception:
        pass

def registry_discover_nodes(timeout: float = 2.0) -> list[tuple[str, int]]:
    """Return only reachable nodes from registry, auto-clean dead entries."""
    if not os.path.exists(REGISTRY_PATH):
        return []
    try:
        with open(REGISTRY_PATH, "r") as f:
            nodes = json.load(f)
        alive = []
        for h, p in nodes:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(0.5)
            try:
                s.connect((h, int(p)))
                alive.append((h, int(p)))
            except Exception:
                print(f"[CLEANUP] Removing dead node {h}:{p}")
            finally:
                s.close()
        _save_registry(alive)
        return alive
    except Exception:
        return []
