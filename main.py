import sys
import threading
from overlay.node import Node
from application.subscriber.subscriber import Subscriber


def start_node(node: Node):
    """Thread target for starting the node server."""
    try:
        node.start_server()
    except Exception as e:
        print(f"[FATAL] Node crashed: {e}")

def parse_topics(arg: str):
    """Helper to parse comma-separated topics."""
    if not arg:
        return []
    return [t.strip() for t in arg.split(",") if t.strip()]

if __name__ == "__main__":
    # ----------------------------------------------------------------------
    # Usage
    # ----------------------------------------------------------------------
    if len(sys.argv) < 2:
        print("Usage: python main.py <port> [peer1_port peer2_port ...] "
              "[--topics t1,t2] [--no-interactive] [--latency-log]")
        print("\nOptions:")
        print("  --topics <list>     Auto-subscribe without prompting")
        print("  --no-interactive    Disable interactive command mode")
        print("  --latency-log       Enable latency logging")
        sys.exit(1)

    # ----------------------------------------------------------------------
    # Parse flags
    # ----------------------------------------------------------------------
    enable_latency_log = "--latency-log" in sys.argv
    no_interactive = "--no-interactive" in sys.argv

    # Extract topics if provided
    auto_topics = []
    if "--topics" in sys.argv:
        idx = sys.argv.index("--topics")
        if idx + 1 < len(sys.argv):
            auto_topics = parse_topics(sys.argv[idx + 1])

    # Extract non-flag arguments
    args = []
    skip_next = False
    for i, arg in enumerate(sys.argv[1:], start=1):
        if skip_next:
            skip_next = False
            continue
        if arg in ("--latency-log", "--no-interactive"):
            continue
        if arg == "--topics":
            skip_next = True
            continue
        args.append(arg)

    # ----------------------------------------------------------------------
    # Port + Peers
    # ----------------------------------------------------------------------
    host = "127.0.0.1"
    port = int(args[0])
    peers = [(host, int(p)) for p in args[1:]] if len(args) > 1 else []

    print(f"[BOOT] Starting node at {host}:{port} with peers: {peers}")

    # ----------------------------------------------------------------------
    # Create Node + Subscriber
    # ----------------------------------------------------------------------
    node = Node(host, port, peers)
    sub = Subscriber(node, name=f"Subscriber@{port}", enable_latency_log=enable_latency_log)

    # ----------------------------------------------------------------------
    # Topic subscription logic
    # ----------------------------------------------------------------------
    if auto_topics:
        # Non-interactive auto-subscription (for automated tests)
        for t in auto_topics:
            sub.subscribe(t)
        print(f"[AUTO] Subscribed to topics (non-interactive): {', '.join(auto_topics)}")

    else:
        # Interactive mode
        print("\n📡 Available topics: news, updates, alerts, changes, weather, sports")
        print("Enter topics to subscribe (comma-separated, e.g. news,updates):")
        topic_input = input("> ").strip()

        if topic_input:
            topics = parse_topics(topic_input)
            for t in topics:
                sub.subscribe(t)
        else:
            sub.subscribe("news")  # default fallback

    # ----------------------------------------------------------------------
    # Assign subscriber topics to node
    # ----------------------------------------------------------------------
    node.subscriber = sub
    node.discovery.node = node

    # ----------------------------------------------------------------------
    # Start node server in background
    # ----------------------------------------------------------------------
    threading.Thread(target=start_node, args=(node,), daemon=True).start()

    print(f"\n[READY] Node running at {host}:{port}")
    print("Subscribed to topics:", ", ".join(sub.subscriptions))
    print("Press Ctrl+C to stop.\n")

    # ----------------------------------------------------------------------
    # Optional interactive mode
    # ----------------------------------------------------------------------
    if no_interactive:
        print("[INFO] Interactive mode disabled (--no-interactive).")
        try:
            while True:
                pass  # Keep the process alive
        except KeyboardInterrupt:
            print(f"\n[SHUTDOWN] Node {port} stopped.")
        sys.exit(0)

    # Interactive subscription management (default)
    print("\n=== Interactive Subscription Management ===")
    print("Commands:")
    print("  subscribe <topic>   - Subscribe to a new topic")
    print("  unsubscribe <topic> - Unsubscribe from a topic")
    print("  list                - Show current subscriptions")
    print("  quit                - Stop the node\n")

    try:
        while True:
            try:
                cmd = input("> ").strip()
                if not cmd:
                    continue

                if cmd.startswith("subscribe "):
                    topic = cmd.split(" ", 1)[1].strip()
                    sub.subscribe(topic)

                elif cmd.startswith("unsubscribe "):
                    topic = cmd.split(" ", 1)[1].strip()
                    sub.unsubscribe(topic)

                elif cmd == "list":
                    print(f"Current subscriptions: {', '.join(sorted(sub.subscriptions))}")

                elif cmd == "quit":
                    print(f"\n[SHUTDOWN] Node {port} stopping...")
                    break

                else:
                    print(f"[ERROR] Unknown command: '{cmd}'")

            except EOFError:
                print(f"\n[SHUTDOWN] Node {port} stopping...")
                break
            except Exception as e:
                print(f"[ERROR] Command failed: {e}")

    except KeyboardInterrupt:
        print(f"\n[SHUTDOWN] Node {port} stopped.")
