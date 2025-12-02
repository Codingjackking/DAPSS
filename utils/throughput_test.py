"""
Throughput Test

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
        "content": f"Throughput test message {msg_id}",
        "sender": "ThroughputTest",
        "timestamp": datetime.now().isoformat(),
        "lamport_timestamp": 0
    }

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1.0)
            s.connect(("127.0.0.1", port))

            payload = json.dumps(message).encode("utf-8")
            header = struct.pack("!I", len(payload))
            s.sendall(header + payload)
        return True
    except:
        return False


def main():
    if len(sys.argv) < 3:
        print("Usage: python simple_throughput_test.py <publisher_port> <duration_seconds>")
        print("\nExample:")
        print("  python simple_throughput_test.py 5001 10")
        sys.exit(1)

    publisher_port = int(sys.argv[1])
    duration = int(sys.argv[2])

    print("="*60)
    print("DAPSS Throughput Test")
    print("="*60)
    print(f"Publisher Port: {publisher_port}")
    print(f"Duration: {duration} seconds")
    print(f"\nThis will send messages as fast as possible to measure throughput.\n")

    try:
        input("Press Enter to start (Ctrl+C to cancel)...")
    except KeyboardInterrupt:
        print("\nCancelled")
        return

    print(f"\n[THROUGHPUT] Sending messages for {duration} seconds...")
    print("="*60)

    start_time = time.time()
    msg_count = 0
    failed_count = 0

    while time.time() - start_time < duration:
        if send_message(publisher_port, "throughput_test", msg_count):
            msg_count += 1
            if msg_count % 100 == 0:
                elapsed = time.time() - start_time
                current_rate = msg_count / elapsed
                print(f"[{elapsed:.1f}s] Sent {msg_count} messages ({current_rate:.1f} msg/s)")
        else:
            failed_count += 1

    elapsed = time.time() - start_time

    print("="*60)
    print(f"\n[RESULTS]")
    print(f"  Duration: {elapsed:.2f} seconds")
    print(f"  Total Sent: {msg_count}")
    print(f"  Failed: {failed_count}")
    print(f"  Throughput: {msg_count / elapsed:.2f} messages/second")
    print(f"  Avg Interval: {(elapsed / msg_count * 1000):.2f} ms/message")

    print("\n" + "="*60)


if __name__ == "__main__":
    main()
