import sys
import threading
import time
from overlay.node import Node
from subscriber.subscriber import Subscriber


def start_node(node: Node):
    """Thread target for starting the node server."""
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

    # --- Create Node (but do not start server yet) ---
    node = Node(host, port, peers)
    sub = Subscriber(node, name=f"Subscriber@{port}")

    # --- Dynamic subscription setup ---
    print("\n📡 Available topics: news, updates, alerts, changes, weather, sports")
    print("Enter topics to subscribe (comma-separated, e.g. news,updates):")
    topic_input = input("> ").strip()

    if topic_input:
        topics = [t.strip() for t in topic_input.split(",") if t.strip()]
        for t in topics:
            sub.subscribe(t)
    else:
        topics = ["news"]
        sub.subscribe("news")  # default fallback

    # --- Assign topics to node before starting ---
    node.subscriber = sub  # ensure Node knows subscriber topics
    node.discovery.node = node  # make sure discovery has access to this node

    # --- Start node server thread ---
    threading.Thread(target=start_node, args=(node,), daemon=True).start()

    print(f"\n[READY] Node running at {host}:{port}")
    print("Subscribed to topics:", ", ".join(sub.subscriptions))
    print("Press Ctrl+C to stop.\n")

    # --- Keep node alive ---
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\n[SHUTDOWN] Node {port} stopped.")
