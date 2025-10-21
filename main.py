import threading
import time

from common.message import Message
from overlay.node import Node
from publisher.publisher import Publisher
from subscriber.subscriber import Subscriber


if __name__ == "__main__":
    # Build a small ring: 2 nodes gossiping to each other
    node1 = Node("127.0.0.1", 5001, [("127.0.0.1", 5002)])
    node2 = Node("127.0.0.1", 5002, [("127.0.0.1", 5001)])

    # Start servers
    threading.Thread(target=node1.start_server, daemon=True).start()
    threading.Thread(target=node2.start_server, daemon=True).start()

    # Attach subscribers and subscribe to a topic
    sub1 = Subscriber(node1, "Alice")
    sub2 = Subscriber(node2, "Bob")
    sub1.subscribe("news")
    sub2.subscribe("news")

    # Give servers a moment to start
    time.sleep(1.0)

    # Publish two ways: via Message and via (topic, content)
    pub = Publisher(node1)
    msg = Message("news", "Distributed systems are powerful!", "Node1")
    pub.publish(msg)

    time.sleep(0.5)
    pub.publish("news", "Gossip works across peers")

    # Keep the process alive briefly to see gossip propagation
    time.sleep(2.0)
