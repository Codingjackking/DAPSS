from overlay.discovery import register_node
import os
import json
import time

class Subscriber:
    """
    Receives and optionally re-gossips messages.
    Supports dynamic subscribe/unsubscribe that updates discovery.
    """

    def __init__(self, node, name: str | None = None, enable_latency_log: bool = False):
        self.node = node
        self.name = name or f"Subscriber@{node.host}:{node.port}"
        self.subscriptions: set[str] = set()
        node.subscriber = self

        # Latency logging support (enabled via command-line flag or environment variable)
        self.latency_log_enabled = enable_latency_log or os.getenv("DAPSS_LATENCY_LOG") == "1"
        self.latency_log_file = f"latency_log_{node.port}.jsonl" if self.latency_log_enabled else None

        if self.latency_log_enabled:
            print(f"[LATENCY] Logging enabled → {self.latency_log_file}")

    # ------------------------------------------------------------
    def subscribe(self, topic: str) -> bool:
        """Subscribe to a new topic dynamically."""
        topic = topic.strip()
        if not topic:
            return False

        if topic in self.subscriptions:
            print(f"[{self.name}] Already subscribed to {topic}")
            return True

        # Phase 1: Startup (consensus not running yet)
        if not self.node.consensus.running:
            self.subscriptions.add(topic)
            print(f"[{self.name}] Subscribed to {topic} (local)")
            self._update_discovery_registry()
            return True

        # Phase 2: Dynamic (consensus is running - use agreement protocol)
        print(f"[{self.name}] Requesting consensus to subscribe to '{topic}'...")
        entry = self.node.consensus.state_manager.create_entry(
            action="subscribe",
            node_id=f"{self.node.host}:{self.node.port}",
            topic=topic
        )

        success = self.node.consensus.request_state_change(entry)
        if success:
            self.subscriptions.add(topic)
            print(f"[{self.name}]  Subscribed to {topic} (consensus reached)")
            self._update_discovery_registry()
            return True
        else:
            print(f"[{self.name}]  Failed to subscribe to {topic} (no consensus)")
            return False

    # ------------------------------------------------------------
    def unsubscribe(self, topic: str) -> bool:
        """Unsubscribe from a topic dynamically."""
        topic = topic.strip()

        if topic not in self.subscriptions:
            print(f"[{self.name}] Not currently subscribed to {topic}")
            return False

        # Phase 1: Startup (consensus not running yet)
        if not self.node.consensus.running:
            self.subscriptions.remove(topic)
            print(f"[{self.name}] Unsubscribed from {topic} (local)")
            self._update_discovery_registry()
            return True

        # Phase 2: Dynamic (consensus is running - use agreement protocol)
        print(f"[{self.name}] Requesting consensus to unsubscribe from '{topic}'...")
        entry = self.node.consensus.state_manager.create_entry(
            action="unsubscribe",
            node_id=f"{self.node.host}:{self.node.port}",
            topic=topic
        )

        success = self.node.consensus.request_state_change(entry)
        if success:
            self.subscriptions.remove(topic)
            print(f"[{self.name}] Unsubscribed from {topic} (consensus reached)")
            self._update_discovery_registry()
            return True
        else:
            print(f"[{self.name}] Failed to unsubscribe from {topic} (no consensus)")
            return False

    # ------------------------------------------------------------
    def receive_message(self, topic: str, content: str) -> None:
        """Deliver a message to the subscriber."""
        if topic in self.subscriptions:
            current_clock = self.node.lamport_clock.get_time()
            receive_time = time.time()

            print(f"[{self.name}] Received '{topic}': {content} [Lamport:{current_clock}]")

            # Log latency data if enabled (for benchmarking)
            if self.latency_log_enabled:
                try:
                    # Try to extract send_time from content if it's JSON
                    content_data = json.loads(content)
                    if "send_time" in content_data:
                        latency_ms = (receive_time - content_data["send_time"]) * 1000
                        log_entry = {
                            "topic": topic,
                            "receive_time": receive_time,
                            "send_time": content_data["send_time"],
                            "latency_ms": latency_ms,
                            "msg_id": content_data.get("msg_id", None)
                        }
                        with open(self.latency_log_file, "a") as f:
                            f.write(json.dumps(log_entry) + "\n")
                except:
                    pass  # Content not JSON or no send_time, skip logging
        else:
            # You could choose to silently ignore or log filtered messages
            print(f"[INFO] Ignored message for unsubscribed topic '{topic}'")

    # ------------------------------------------------------------
    def gossip(self, message_json: str) -> None:
        """Forward a message via the node's gossip protocol."""
        self.node.gossip.gossip_message(message_json)

    # ------------------------------------------------------------
    def _update_discovery_registry(self):
        """Update discovery registry with new topic list."""
        try:
            register_node(
                self.node.host,
                self.node.port,
                list(self.subscriptions)  # convert set → list for JSON
            )
        except Exception as e:
            print(f"[WARN] Could not update discovery topics: {e}")
