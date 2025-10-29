import sys
import threading
import time
from overlay.node import Node
from subscriber.subscriber import Subscriber

def start_node(node: Node):
    try:
        node.start_server()
    except Exception as e:
        print(f"[FATAL] Node crashed: {e}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python main.py <port> [peer1_port peer2_port ...]")
        sys.exit(1)

    host = "127.0.0.1"
    port = int(sys.argv[1])
    peers = [(host, int(p)) for p in sys.argv[2:]] if len(sys.argv) > 2 else []
    print(f"[BOOT] Starting node at {host}:{port} with peers: {peers}")

    node = Node(host, port, peers)
    threading.Thread(target=start_node, args=(node,), daemon=True).start()
    sub = Subscriber(node, name=f"Subscriber@{port}")
    sub.subscribe("news")

    print(f"[READY] Node running at {host}:{port}. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"[SHUTDOWN] Node {port} stopped.")
