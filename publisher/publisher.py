from typing import Optional, Union
from feature.message import Message


class Publisher:
    """High-level API for producing and gossiping messages."""
    def __init__(self, node, name: Optional[str] = None):
        self.node = node
        self.name = name or f"Publisher@{node.host}:{node.port}"

    def publish(self, topic_or_message: Union[str, Message], content: Optional[str] = None) -> None:
        if isinstance(topic_or_message, Message):
            msg = topic_or_message
        else:
            if content is None:
                raise ValueError("content must be provided when using (topic, content)")
            msg = Message(topic_or_message, content, sender=self.name)
        print(f"[{self.name}] Publishing '{msg.topic}': {msg.content}")
        self.node.gossip.gossip_message(msg.to_json())
