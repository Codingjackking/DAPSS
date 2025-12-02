import socket, struct, json

fake_secure_msg = {
    "type": "SECURE",
    "data": "INVALID_ENCRYPTION_BLOB"
}

payload = json.dumps(fake_secure_msg).encode("utf-8")
header = struct.pack("!I", len(payload))

s = socket.socket()
s.connect(("127.0.0.1", 5001))
s.sendall(header + payload)
s.close()

print("[TAMPER] Sent forged SECURE message with invalid encryption")
