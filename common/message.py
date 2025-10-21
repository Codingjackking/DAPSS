import json
from datetime import datetime

class Message:
    """Immutable-ish message DTO with JSON (de)serialization."""
    def __init__(self, topic, content, sender, timestamp=None):
        self.topic = topic
        self.content = content
        self.sender = sender
        self.timestamp = timestamp or datetime.now().isoformat()

    def to_json(self) -> str:
        return json.dumps({
            "topic": self.topic,
            "content": self.content,
            "sender": self.sender,
            "timestamp": self.timestamp
        })

    @staticmethod
    def from_json(data: str) -> "Message":
        obj = json.loads(data)
        return Message(
            topic=obj["topic"],
            content=obj["content"],
            sender=obj["sender"],
            timestamp=obj.get("timestamp")
        )
