"""
Security Module for DAPSS
Provides authentication, message signing, and encryption for distributed nodes.
"""

import hashlib
import hmac
import secrets
import json
import time
from typing import Optional, Dict, Any, Tuple
from cryptography.fernet import Fernet
import base64


class ClusterAuth:
    """
    Handles node authentication using shared cluster secret.

    Authentication Flow:
    1. Node joins cluster with shared secret
    2. Node sends CHALLENGE to peer
    3. Peer responds with HMAC(secret + challenge)
    4. Node verifies HMAC to authenticate peer
    """

    def __init__(self, cluster_secret: str):
        """
        Initialize cluster authentication.

        Args:
            cluster_secret: Shared secret known by all legitimate cluster nodes
        """
        self.cluster_secret = cluster_secret.encode('utf-8')
        self._authenticated_peers = {}  # {(host, port): timestamp}
        self._challenge_cache = {}  # {challenge_id: timestamp}
        self._challenge_timeout = 30.0  # seconds

    # =========================================================================
    # Authentication Challenge/Response
    # =========================================================================

    def create_challenge(self) -> str:
        """
        Generate a random challenge for authentication.

        Returns:
            Random challenge string (32 bytes hex)
        """
        challenge = secrets.token_hex(32)
        self._challenge_cache[challenge] = time.time()
        return challenge

    def create_auth_response(self, challenge: str, node_id: str) -> str:
        """
        Create authentication response to a challenge.

        Args:
            challenge: Challenge string received from peer
            node_id: This node's identifier (host:port)

        Returns:
            HMAC signature proving knowledge of cluster secret
        """
        # Response = HMAC(secret, challenge + node_id + timestamp)
        timestamp = str(int(time.time()))
        message = f"{challenge}:{node_id}:{timestamp}".encode('utf-8')
        signature = hmac.new(self.cluster_secret, message, hashlib.sha256).hexdigest()

        return f"{signature}:{timestamp}"

    def verify_auth_response(self, challenge: str, node_id: str, response: str) -> bool:
        """
        Verify authentication response from a peer.

        Args:
            challenge: Challenge we sent to the peer
            node_id: Peer's identifier (host:port)
            response: Peer's response (signature:timestamp)

        Returns:
            True if authentication successful, False otherwise
        """
        # Check if challenge exists and hasn't expired
        if challenge not in self._challenge_cache:
            print("[AUTH] Challenge not found or expired")
            return False

        challenge_time = self._challenge_cache[challenge]
        if time.time() - challenge_time > self._challenge_timeout:
            print("[AUTH] Challenge expired")
            del self._challenge_cache[challenge]
            return False

        try:
            signature, timestamp = response.split(':', 1)

            # Verify timestamp is recent (within 60 seconds)
            if abs(time.time() - int(timestamp)) > 60:
                print("[AUTH] Response timestamp too old")
                return False

            # Recreate expected signature
            message = f"{challenge}:{node_id}:{timestamp}".encode('utf-8')
            expected_sig = hmac.new(self.cluster_secret, message, hashlib.sha256).hexdigest()

            # Constant-time comparison to prevent timing attacks
            if hmac.compare_digest(signature, expected_sig):
                # Clean up challenge
                del self._challenge_cache[challenge]
                return True
            else:
                print("[AUTH] Signature verification failed")
                return False

        except Exception as e:
            print(f"[AUTH] Error verifying response: {e}")
            return False

    def mark_authenticated(self, peer: Tuple[str, int]) -> None:
        """
        Mark a peer as authenticated.

        Args:
            peer: (host, port) tuple
        """
        self._authenticated_peers[peer] = time.time()
        print(f"[AUTH] ✓ Peer {peer[0]}:{peer[1]} authenticated")

    def is_authenticated(self, peer: Tuple[str, int]) -> bool:
        """
        Check if a peer is authenticated.

        Args:
            peer: (host, port) tuple

        Returns:
            True if peer is authenticated
        """
        return peer in self._authenticated_peers

    def revoke_authentication(self, peer: Tuple[str, int]) -> None:
        """
        Revoke authentication for a peer.

        Args:
            peer: (host, port) tuple
        """
        if peer in self._authenticated_peers:
            del self._authenticated_peers[peer]
            print(f"[AUTH] Revoked authentication for {peer[0]}:{peer[1]}")

    # =========================================================================
    # Message Signing (HMAC)
    # =========================================================================

    def sign_message(self, message_data: str) -> str:
        """
        Sign a message with HMAC for integrity verification.

        Args:
            message_data: Message content (JSON string)

        Returns:
            HMAC signature (hex string)
        """
        signature = hmac.new(
            self.cluster_secret,
            message_data.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return signature

    def verify_signature(self, message_data: str, signature: str) -> bool:
        """
        Verify message signature.

        Args:
            message_data: Message content (JSON string)
            signature: HMAC signature to verify

        Returns:
            True if signature is valid
        """
        expected_sig = self.sign_message(message_data)
        return hmac.compare_digest(signature, expected_sig)

    def wrap_message(self, message_json: str) -> str:
        """
        Wrap a message with signature for secure transmission.

        Args:
            message_json: Original message JSON

        Returns:
            Wrapped message with signature
        """
        signature = self.sign_message(message_json)
        wrapped = {
            "payload": message_json,
            "signature": signature,
            "signed_at": int(time.time())
        }
        return json.dumps(wrapped)

    def unwrap_message(self, wrapped_json: str) -> Optional[str]:
        """
        Unwrap and verify a signed message.

        Args:
            wrapped_json: Wrapped message with signature

        Returns:
            Original message JSON if signature valid, None otherwise
        """
        try:
            wrapped = json.loads(wrapped_json)
            payload = wrapped["payload"]
            signature = wrapped["signature"]
            signed_at = wrapped["signed_at"]

            # Check message age (reject messages older than 5 minutes)
            if time.time() - signed_at > 300:
                print("[AUTH] Message too old, rejected")
                return None

            if self.verify_signature(payload, signature):
                return payload
            else:
                print("[AUTH] ✗ Message signature verification failed")
                return None

        except Exception as e:
            print(f"[AUTH] Error unwrapping message: {e}")
            return None


class MessageEncryption:
    """
    Handles message encryption/decryption using Fernet (AES-128 in CBC mode).

    Note: Requires 'cryptography' package
    Install: pip install cryptography
    """

    def __init__(self, encryption_key: Optional[str] = None):
        """
        Initialize encryption.

        Args:
            encryption_key: Base64-encoded Fernet key, or None to generate new key
        """
        if encryption_key:
            self.key = encryption_key.encode('utf-8')
        else:
            self.key = Fernet.generate_key()

        self.cipher = Fernet(self.key)

    def get_key(self) -> str:
        """
        Get the encryption key (for sharing with cluster nodes).

        Returns:
            Base64-encoded encryption key
        """
        return self.key.decode('utf-8')

    def encrypt(self, message_json: str) -> str:
        """
        Encrypt a message.

        Args:
            message_json: Message to encrypt (JSON string)

        Returns:
            Encrypted message (base64 string)
        """
        encrypted = self.cipher.encrypt(message_json.encode('utf-8'))
        return base64.b64encode(encrypted).decode('utf-8')

    def decrypt(self, encrypted_message: str) -> Optional[str]:
        """
        Decrypt a message.

        Args:
            encrypted_message: Encrypted message (base64 string)

        Returns:
            Decrypted message JSON, or None if decryption failed
        """
        try:
            encrypted = base64.b64decode(encrypted_message.encode('utf-8'))
            decrypted = self.cipher.decrypt(encrypted)
            return decrypted.decode('utf-8')
        except Exception as e:
            print(f"[CRYPTO] Decryption failed: {e}")
            return None


class SecureChannel:
    """
    Combines authentication, signing, and encryption for secure node communication.
    """

    def __init__(self, cluster_secret: str, encryption_key: Optional[str] = None):
        """
        Initialize secure channel.

        Args:
            cluster_secret: Shared secret for authentication
            encryption_key: Encryption key (optional, will generate if None)
        """
        self.auth = ClusterAuth(cluster_secret)
        self.crypto = MessageEncryption(encryption_key)

    def secure_message(self, message_json: str) -> str:
        """
        Apply full security: sign + encrypt.

        Args:
            message_json: Original message JSON

        Returns:
            Secured message
        """
        # Step 1: Sign the message
        signed = self.auth.wrap_message(message_json)

        # Step 2: Encrypt the signed message
        encrypted = self.crypto.encrypt(signed)

        # Step 3: Wrap in transport envelope
        envelope = {
            "type": "SECURE",
            "data": encrypted
        }
        return json.dumps(envelope)

    def unsecure_message(self, envelope_json: str) -> Optional[str]:
        """
        Decrypt and verify a secured message.

        Args:
            envelope_json: Secured message envelope

        Returns:
            Original message JSON if valid, None otherwise
        """
        try:
            envelope = json.loads(envelope_json)

            if envelope.get("type") != "SECURE":
                print("[SECURE] Invalid envelope type")
                return None

            encrypted = envelope["data"]

            # Step 1: Decrypt
            signed = self.crypto.decrypt(encrypted)
            if not signed:
                return None

            # Step 2: Verify signature
            original = self.auth.unwrap_message(signed)
            if not original:
                return None

            return original

        except Exception as e:
            print(f"[SECURE] Error processing message: {e}")
            return None
