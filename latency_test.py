"""
Latency Test
"""

import socket
import json
import time
import struct
import sys
from datetime import datetime


def send_message(port: int, topic: str, msg_id: int) -> bool:
    """Send a timestamped message for latency measurement"""
    send_time = time.time()

    message = {
        "topic": topic,
        "content": json.dumps({
            "msg_id": msg_id,
            "text": f"Latency test message {msg_id}",
            "send_time": send_time  # Include send timestamp in content
        }),
        "sender": "LatencyTest",
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
        print("Usage: python latency_test.py <publisher_port> <topic> <num_messages>")
        print("\nExample:")
        print("  python latency_test.py 5001 latency_test 50")
        print("\nIMPORTANT: Start your subscriber nodes with --latency-log flag:")
        print("  python main.py 5002 5001 --latency-log")
        print("  python main.py 5003 5001 --latency-log")
        sys.exit(1)

    publisher_port = int(sys.argv[1])
    topic = sys.argv[2]
    num_messages = int(sys.argv[3])

    print("="*60)
    print("DAPSS Latency Test")
    print("="*60)
    print(f"Publisher Port: {publisher_port}")
    print(f"Topic: {topic}")
    print(f"Messages: {num_messages}")
    print("\nMake sure subscriber nodes were started with --latency-log flag\n")

    try:
        input("Press Enter to start test (Ctrl+C to cancel)...")
    except KeyboardInterrupt:
        print("\nCancelled")
        return

    print(f"\n[SEND] Sending {num_messages} timestamped messages...")
    print("="*60)

    sent_count = 0
    start_time = time.time()

    for msg_id in range(num_messages):
        if send_message(publisher_port, topic, msg_id):
            sent_count += 1
            if (msg_id + 1) % 10 == 0:
                print(f"[{msg_id + 1:03d}/{num_messages}] Sent {msg_id + 1} messages")

        # Small delay between messages
        time.sleep(0.05)  # 50ms delay

    elapsed = time.time() - start_time

    print("="*60)
    print("\n[RESULTS]")
    print(f"  Sent: {sent_count}/{num_messages}")
    print(f"  Duration: {elapsed:.2f}s")

    print("\n[NEXT STEPS]")
    print("  1. Wait a few seconds for message propagation")
    print("  2. Run the analysis script:")
    print("     python analyze_latency.py")
    print("\n  This will analyze latency_log_*.jsonl files and show statistics")

    print("\n" + "="*60)


if __name__ == "__main__":
    main()
