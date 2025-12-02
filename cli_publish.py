import sys
import socket
import struct
from overlay.feature.message import Message
from overlay.discovery import registry_discover_nodes, discover_nodes as udp_discover


def send_message(host: str, port: int, msg_json: str):
    payload = msg_json.encode("utf-8")
    header = struct.pack("!I", len(payload))  # 4-byte big-endian header
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(2.5)
            s.connect((host, port))
            s.sendall(header + payload)
        print(f"[OK] Sent to {host}:{port}")
    except Exception as e:
        print(f"[ERROR] {host}:{port} → {e}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python cli_publish.py <topic> <content>")
        sys.exit(1)

    topic, content = sys.argv[1], sys.argv[2]

    # 1) Prefer registry (topic-aware)
    active_nodes = registry_discover_nodes(topic_filter=[topic], timeout=2.0)

    # 2) Fallback to UDP discovery if registry empty
    if not active_nodes:
        print("[INFO] Registry empty; falling back to UDP discovery...")
        active_nodes = udp_discover(timeout=2.0, topic_filter=[topic])

    if not active_nodes:
        print("[WARN] No active nodes detected for topic '{topic}'. Exiting.")
        sys.exit(0)

    print(f"[INFO] Found {len(active_nodes)} active node(s): {active_nodes}")

    # Gateway Model: Send to ONE node instead of all (prevents duplicate timestamps)
    # The gateway node will assign a proper Lamport timestamp and gossip to the cluster
    gateway_node = active_nodes[0]  # Pick first node as gateway

    msg = Message(topic, content, sender="CLI_Publisher", lamport_timestamp=0).to_json()

    print(f"[INFO] Using gateway node: {gateway_node[0]}:{gateway_node[1]}")
    send_message(gateway_node[0], gateway_node[1], msg)

    print(f"[DONE] Sent '{topic}' to gateway node {gateway_node[0]}:{gateway_node[1]}")
