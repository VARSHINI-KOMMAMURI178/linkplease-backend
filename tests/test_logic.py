"""
Lightweight unit tests for the pure-function pieces (no DB, no network).
Run with: pytest tests/
"""
import hmac
import hashlib
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.security import verify_signature
from app.event_processor import keyword_matches


def test_verify_signature_valid():
    secret = "shh"
    body = b'{"a":1}'
    sig = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    assert verify_signature(body, f"sha256={sig}", secret) is True


def test_verify_signature_invalid():
    secret = "shh"
    body = b'{"a":1}'
    assert verify_signature(body, "sha256=deadbeef", secret) is False


def test_verify_signature_missing_header():
    assert verify_signature(b"x", "", "shh") is False


def test_verify_signature_wrong_secret():
    body = b'{"a":1}'
    sig = hmac.new(b"right", body, hashlib.sha256).hexdigest()
    assert verify_signature(body, f"sha256={sig}", "wrong") is False


def test_keyword_matches_case_insensitive():
    assert keyword_matches("PRICE please 🙏", "price") is True
    assert keyword_matches("what's the Price?", "PRICE") is True


def test_keyword_matches_substring_anywhere():
    assert keyword_matches("hey can u tell me the price pls", "price") is True


def test_keyword_matches_no_match():
    assert keyword_matches("hello there", "price") is False


def test_keyword_matches_empty_inputs():
    assert keyword_matches("", "price") is False
    assert keyword_matches("price", "") is False
