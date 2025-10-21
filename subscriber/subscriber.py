class Subscriber:
    """
    Manages topic subscriptions and handles local delivery callbacks.
    If a subscriber wants to re-broadcast (gossip) a received message,
    it should call self.node.gossip.gossip_message(message_json).
    """

    def __init__(self, node, name: str | None = None):
        self.node = node
        self.name = name or f"Subscriber@{node.host}:{node.port}"
        self.subscriptions: set[str] = set()
        node.subscriber = self  # link back

    def subscribe(self, topic: str) -> None:
        self.subscriptions.add(topic)
        print(f"[{self.name}] Subscribed to: {topic}")

    def unsubscribe(self, topic: str) -> None:
        if topic in self.subscriptions:
            self.subscriptions.remove(topic)
            print(f"[{self.name}] Unsubscribed from: {topic}")

    def receive_message(self, topic: str, content: str) -> None:
        """Application-level delivery hook for locally delivered gossip."""
        print(f"[{self.name}] Received '{topic}': {content}")

    def gossip(self, message_json: str) -> None:
        """
        Explicitly gossip a raw message (if subscriber wants to re-share it).
        For instance, could be used for collaborative gossiping.
        """
        self.node.gossip.gossip_message(message_json)
