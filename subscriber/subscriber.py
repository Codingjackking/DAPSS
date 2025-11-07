from overlay.discovery import register_node

class Subscriber:
    """
    Receives and optionally re-gossips messages.
    Supports dynamic subscribe/unsubscribe that updates discovery.
    """

    def __init__(self, node, name: str | None = None):
        self.node = node
        self.name = name or f"Subscriber@{node.host}:{node.port}"
        self.subscriptions: set[str] = set()
        node.subscriber = self

    # ------------------------------------------------------------
    def subscribe(self, topic: str) -> None:
        """Subscribe to a new topic dynamically."""
        topic = topic.strip()
        if not topic:
            return
        if topic not in self.subscriptions:
            self.subscriptions.add(topic)
            print(f"[{self.name}] Subscribed to {topic}")
            self._update_discovery_registry()
        else:
            print(f"[{self.name}] Already subscribed to {topic}")

    # ------------------------------------------------------------
    def unsubscribe(self, topic: str) -> None:
        """Unsubscribe from a topic dynamically."""
        topic = topic.strip()
        if topic in self.subscriptions:
            self.subscriptions.remove(topic)
            print(f"[{self.name}] Unsubscribed from {topic}")
            self._update_discovery_registry()
        else:
            print(f"[{self.name}] Not currently subscribed to {topic}")

    # ------------------------------------------------------------
    def receive_message(self, topic: str, content: str) -> None:
        """Deliver a message to the subscriber."""
        if topic in self.subscriptions:
            current_clock = self.node.lamport_clock.get_time()
            print(f"[{self.name}] Received '{topic}': {content} [Lamport:{current_clock}]")
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
