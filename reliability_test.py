"""
Reliability Test
"""

import socket
import json
import time
import struct
import sys
from datetime import datetime


def send_message(port: int, topic: str, msg_id: int) -> bool:
    """Send a single test message"""
    message = {
        "topic": topic,
        "content": json.dumps({"msg_id": msg_id, "text": f"Test message {msg_id}"}),
        "sender": "ReliabilityTest",
        "timestamp": datetime.now().isoformat(),
        "lamport_timestamp": 0
    }

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(2.0)
            s.connect(("127.0.0.1", port))

            payload = json.dumps(message).encode("utf-8")
            header = struct.pack("!I", len(payload))
            s.sendall(header + payload)
        return True
    except Exception as e:
        print(f"[ERROR] Failed to send message {msg_id}: {e}")
        return False


def main():
    if len(sys.argv) < 4:
        print("Usage: python simple_reliability_test.py <publisher_port> <topic> <num_messages>")
        print("\nExample:")
        print("  python simple_reliability_test.py 5001 reliability_test 20")
        print("\nMake sure your nodes are subscribed to the topic before running!")
        sys.exit(1)

    publisher_port = int(sys.argv[1])
    topic = sys.argv[2]
    num_messages = int(sys.argv[3])

    print("="*60)
    print("DAPSS Reliability Test")
    print("="*60)
    print(f"Publisher Port: {publisher_port}")
    print(f"Topic: {topic}")
    print(f"Messages: {num_messages}")
    print("\nMake sure all your subscriber nodes are running and")
    print(f"subscribed to '{topic}' before continuing!\n")

    try:
        input("Press Enter to start sending messages (Ctrl+C to cancel)...")
    except KeyboardInterrupt:
        print("\nCancelled")
        return

    print(f"\n[SEND] Sending {num_messages} numbered messages...")
    print("="*60)

    sent_count = 0
    failed_count = 0

    for msg_id in range(num_messages):
        if send_message(publisher_port, topic, msg_id):
            sent_count += 1
            print(f"[{msg_id:03d}] Sent message {msg_id}")
        else:
            failed_count += 1

        # Small delay to avoid overwhelming the system
        time.sleep(0.1)

    print("="*60)
    print("\n[RESULTS]")
    print(f"  Sent: {sent_count}/{num_messages}")
    print(f"  Failed: {failed_count}/{num_messages}")

    print("\n[INSTRUCTIONS]")
    print("Check your subscriber node consoles to verify:")
    print(f"  1. All {num_messages} messages were received (msg_id 0 to {num_messages-1})")
    print("  2. Messages appear on all subscriber nodes (gossip works)")
    print("  3. No messages are missing (reliable delivery)")
    print("  4. Check for duplicates (some are expected with gossip)")
    print("  5. Lamport timestamps are increasing")

    print("\n" + "="*60)


if __name__ == "__main__":
    main()
