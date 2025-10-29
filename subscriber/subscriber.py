class Subscriber:
    """Receives and optionally re-gossips messages."""
    def __init__(self, node, name: str | None = None):
        self.node = node
        self.name = name or f"Subscriber@{node.host}:{node.port}"
        self.subscriptions: set[str] = set()
        node.subscriber = self

    def subscribe(self, topic: str) -> None:
        self.subscriptions.add(topic)
        print(f"[{self.name}] Subscribed to {topic}")

    def unsubscribe(self, topic: str) -> None:
        self.subscriptions.discard(topic)
        print(f"[{self.name}] Unsubscribed from {topic}")

    def receive_message(self, topic: str, content: str) -> None:
        print(f"[{self.name}] Received '{topic}': {content}")

    def gossip(self, message_json: str) -> None:
        self.node.gossip.gossip_message(message_json)
