import sys
import socket
import struct
import threading
from feature.message import Message
from overlay.discovery import registry_discover_nodes as discover_nodes


def send_message(host: str, port: int, msg_json: str):
    payload = msg_json.encode("utf-8")
    header = struct.pack("!I", len(payload))
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

    # Use discovery to find live nodes dynamically
    active_nodes = discover_nodes(timeout=2.0)
    if not active_nodes:
        print("[WARN] No active nodes detected! Exiting.")
        sys.exit(0)

    print(f"[INFO] Found {len(active_nodes)} active node(s): {active_nodes}")

    msg = Message(topic, content, sender="CLI_Publisher").to_json()
    threads = [threading.Thread(target=send_message, args=(h, p, msg)) for h, p in active_nodes]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    print(f"[DONE] Broadcasted '{topic}' to {len(active_nodes)} detected node(s).")
