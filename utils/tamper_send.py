"""
tamper_send.py - Security Testing Utility
Sends various types of tampered messages to test the security layer
"""

import sys
import socket
import struct
import json
import time
from datetime import datetime


# ============================================================================
# TAMPERED MESSAGE TEMPLATES
# ============================================================================

TAMPER_TYPES = {
    "secure": {
        "name": "Forged Secure Message",
        "description": "Invalid encrypted message with fake signature",
        "payload": {
            "type": "SECURE",
            "data": "INVALID_ENCRYPTION_BLOB_X7F8A9B",
            "signature": "FORGED_SIGNATURE_12345ABCDEF",
            "timestamp": "2024-01-01T00:00:00"
        }
    },
    
    "malformed": {
        "name": "Malformed JSON Structure",
        "description": "Message with corrupted structure",
        "payload": {
            "topic": "news",
            "content": {"nested": {"deeply": {"invalid": "structure" * 100}}},
            "sender": "malicious_actor",
            "timestamp": datetime.now().isoformat(),
            "lamport_timestamp": -1,  # Invalid negative timestamp
            "INJECTED_FIELD": "<?php system($_GET['cmd']); ?>"
        }
    },
    
    "replay": {
        "name": "Replay Attack",
        "description": "Old message with past timestamp",
        "payload": {
            "topic": "news",
            "content": "replay_attack_content",
            "sender": "attacker",
            "timestamp": "2020-01-01T00:00:00",  # Old timestamp
            "lamport_timestamp": 1
        }
    },
    
    "oversized": {
        "name": "Oversized Message",
        "description": "Extremely large payload (potential DoS)",
        "payload": {
            "topic": "news",
            "content": "A" * 1000000,  # 1MB of data
            "sender": "dos_attacker",
            "timestamp": datetime.now().isoformat(),
            "lamport_timestamp": 999999
        }
    },
    
    "injection": {
        "name": "Code Injection Attempt",
        "description": "Message with potential code injection",
        "payload": {
            "topic": "'; DROP TABLE messages; --",
            "content": "__import__('os').system('whoami')",
            "sender": "{{ 7*7 }}",  # Template injection
            "timestamp": datetime.now().isoformat(),
            "lamport_timestamp": 0,
            "exec": "eval(compile('print(1)', '<string>', 'exec'))"
        }
    },
    
    "consensus_fake": {
        "name": "Fake Consensus Message",
        "description": "Forged Raft consensus message",
        "payload": {
            "type": "VOTE_REQUEST",
            "term": 999999,
            "candidate_id": "attacker:6666",
            "last_log_index": 999,
            "last_log_term": 999
        }
    },
    
    "heartbeat_spoof": {
        "name": "Spoofed Heartbeat",
        "description": "Fake leader heartbeat",
        "payload": {
            "type": "HEARTBEAT",
            "term": 999999,
            "leader_id": "fake_leader:6666",
            "commit_index": 999
        }
    },
    
    "binary": {
        "name": "Binary Payload",
        "description": "Raw binary data instead of JSON",
        "payload": b"\x00\x01\x02\x03\xFF\xFE\xFD\xFC" * 100,
        "raw_binary": True
    }
}


# ============================================================================
# MESSAGE SENDING FUNCTIONS
# ============================================================================

def send_message(port, payload, is_binary=False):
    """Send a tampered message to the target port"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2.0)
        s.connect(("127.0.0.1", port))
        
        if is_binary:
            # Send raw binary data
            data = payload if isinstance(payload, bytes) else payload.encode("utf-8")
            header = struct.pack("!I", len(data))
            s.sendall(header + data)
        else:
            # Send JSON with length prefix
            json_data = json.dumps(payload).encode("utf-8")
            header = struct.pack("!I", len(json_data))
            s.sendall(header + json_data)
        
        s.close()
        return True
        
    except Exception as e:
        print(f"[ERROR] Failed to send: {e}")
        return False


def send_tampered_message(port, tamper_type="secure"):
    """Send a specific type of tampered message"""
    
    if tamper_type not in TAMPER_TYPES:
        print(f"[ERROR] Unknown tamper type: {tamper_type}")
        print(f"Available types: {', '.join(TAMPER_TYPES.keys())}")
        return False
    
    tamper = TAMPER_TYPES[tamper_type]
    print(f"\n[TAMPER] Type: {tamper['name']}")
    print(f"[TAMPER] Description: {tamper['description']}")
    print(f"[TAMPER] Target: 127.0.0.1:{port}")
    
    is_binary = tamper.get("raw_binary", False)
    payload = tamper["payload"]
    
    # Show payload preview (truncated if too large)
    if not is_binary:
        payload_str = json.dumps(payload, indent=2)
        if len(payload_str) > 500:
            preview = payload_str[:250] + "\n... [truncated] ...\n" + payload_str[-250:]
        else:
            preview = payload_str
        print(f"\n[PAYLOAD]\n{preview}\n")
    else:
        print(f"\n[PAYLOAD] Binary data ({len(payload)} bytes)\n")
    
    print("[SENDING]...", end=" ", flush=True)
    success = send_message(port, payload, is_binary)
    
    if success:
        print("✓ Sent")
        print("\n[INFO] Tampered message sent successfully")
        print("[INFO] Check node logs to verify it was rejected/handled safely")
        return True
    else:
        print("✗ Failed")
        return False


def send_all_attacks(port):
    """Send all tamper types in sequence"""
    print(f"\n{'='*60}")
    print("COMPREHENSIVE SECURITY TEST - ALL ATTACK VECTORS")
    print(f"{'='*60}")
    
    results = {}
    for tamper_type in TAMPER_TYPES.keys():
        print(f"\n{'-'*60}")
        success = send_tampered_message(port, tamper_type)
        results[tamper_type] = success
        time.sleep(0.5)  # Brief delay between attacks
    
    # Summary
    print(f"\n{'='*60}")
    print("ATTACK SUMMARY")
    print(f"{'='*60}")
    for tamper_type, success in results.items():
        status = "✓ Sent" if success else "✗ Failed"
        print(f"{status:10} | {TAMPER_TYPES[tamper_type]['name']}")
    
    total = len(results)
    sent = sum(1 for s in results.values() if s)
    print(f"\n{sent}/{total} attack vectors sent successfully")
    print(f"{'='*60}\n")


# ============================================================================
# MAIN
# ============================================================================

def main():
    if len(sys.argv) < 2:
        print("Usage: python tamper_send.py <port> [tamper_type]")
        print("\nExamples:")
        print("  python tamper_send.py 7001                # Default: secure")
        print("  python tamper_send.py 7001 injection      # Specific attack")
        print("  python tamper_send.py 7001 all            # All attacks")
        print("\nAvailable tamper types:")
        for key, value in TAMPER_TYPES.items():
            print(f"  {key:15} - {value['name']}")
        print(f"  {'all':15} - Send all attack types")
        sys.exit(1)

    port = int(sys.argv[1])
    tamper_type = sys.argv[2] if len(sys.argv) > 2 else "secure"
    
    print("="*60)
    print("DAPSS Security Testing - Tamper Detection")
    print("="*60)
    
    if tamper_type == "all":
        send_all_attacks(port)
    else:
        success = send_tampered_message(port, tamper_type)
        if not success:
            sys.exit(1)
    
    print("\n[REMINDER] Check the following logs:")
    print(f"  - gossip_log_{port}.json")
    print(f"  - node_data_{port}/node_log.json")
    print("  - Node console output for security warnings")
    print("\nTampered messages should be rejected or safely handled.\n")


if __name__ == "__main__":
    main()