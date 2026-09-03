"""Backward-compatible auth - delegates to auth.py (credentials from .env only)."""
from auth import APP_ID, APP_SECRET, TOTP_SECRET, USER_ID, PASSWORD, create_client  # noqa: F401

try:
    client, session = create_client()
    print("Successfully logged in!")
    print("Session:", session)
    token = client.get_token()
    print("Token:", token[:24] + "..." if token else None)
except Exception as e:
    session = None
    print(f"Login Error: {e}")
    print("Tip: check .env credentials and that your IP is registered with Arrow")
