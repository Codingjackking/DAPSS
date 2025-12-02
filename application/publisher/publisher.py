from typing import Optional, Union
from overlay.feature.message import Message


class Publisher:
    """High-level API for producing and gossiping messages."""
    def __init__(self, node, name: Optional[str] = None):
        self.node = node
        self.name = name or f"Publisher@{node.host}:{node.port}"

    def publish(self, topic_or_message: Union[str, Message], content: Optional[str] = None) -> None:
        if isinstance(topic_or_message, Message):
            msg = topic_or_message
           
            if msg.lamport_timestamp == 0:
                lamport_ts = self.node.lamport_clock.tick()
                msg = Message(msg.topic, msg.content, msg.sender, msg.timestamp, lamport_ts)
        else:
            if content is None:
                raise ValueError("content must be provided when using (topic, content)")
            # Increment Lamport clock and attach timestamp to message
            lamport_ts = self.node.lamport_clock.tick()
            msg = Message(topic_or_message, content, sender=self.name, lamport_timestamp=lamport_ts)
        print(f"[{self.name}] Publishing '{msg.topic}': {msg.content} [L:{msg.lamport_timestamp}]")
        self.node.gossip.gossip_message(msg.to_json())
