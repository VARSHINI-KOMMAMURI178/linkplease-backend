import hmac
import hashlib


def verify_signature(raw_body: bytes, header_value: str, secret: str) -> bool:
    """
    Verify X-PseudoGram-Signature: sha256=<hex>
    HMAC-SHA256 of the raw request body, keyed with our API key.
    Uses a constant-time comparison to avoid timing side-channels.
    """
    if not header_value or not secret:
        return False
    if not header_value.startswith("sha256="):
        return False
    provided_hex = header_value.split("=", 1)[1].strip()
    expected_hex = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(provided_hex, expected_hex)
