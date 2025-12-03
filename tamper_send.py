import sys
import socket
import struct
import json
import time

def main():
    if len(sys.argv) < 2:
        print("Usage: python tamper_send.py <port>")
        sys.exit(1)

    port = int(sys.argv[1])

    fake_secure_msg = {
        "type": "SECURE",
        "data": "INVALID_ENCRYPTION_BLOB"
    }

    payload = json.dumps(fake_secure_msg).encode("utf-8")
    header = struct.pack("!I", len(payload))

    try:
        s = socket.socket()
        s.settimeout(2.0)
        s.connect(("127.0.0.1", port))
        s.sendall(header + payload)
        s.close()
        print(f"[TAMPER] Sent forged SECURE message to port {port}")
    except Exception as e:
        print(f"[TAMPER ERROR] Failed to send tampered message: {e}")


if __name__ == "__main__":
    main()
