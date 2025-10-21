from typing import Optional, Union

from common.message import Message


class Publisher:
    """Creates messages and hands them to the gossip layer for dissemination."""
    def __init__(self, node, name: Optional[str] = None):
        self.node = node
        self.name = name or f"Publisher@{node.host}:{node.port}"

    def publish(self, topic_or_message: Union[str, Message], content: Optional[str] = None) -> None:
        """
        Accepts either:
          - publish(topic, content)
          - publish(Message)
        """
        if isinstance(topic_or_message, Message):
            msg = topic_or_message
        else:
            if content is None:
                raise ValueError("content must be provided when publishing via (topic, content)")
            msg = Message(topic_or_message, content, sender=self.name)

        print(f"[{self.name}] Publishing on '{msg.topic}': {msg.content}")
        self.node.gossip.gossip_message(msg.to_json())
