"""Authentication helper for Arrow Broker - credentials come from .env ONLY.

Never hardcode secrets here. If one is missing, this fails loudly with a hint
instead of silently using a stale key.
"""
import os
from dotenv import load_dotenv
from pyarrow_client import ArrowClient

load_dotenv()

APP_ID = os.getenv("ARROW_APP_ID", "")
APP_SECRET = os.getenv("ARROW_APP_SECRET", "")
TOTP_SECRET = os.getenv("ARROW_TOTP_SECRET", "")
USER_ID = os.getenv("ARROW_USER_ID", "")
PASSWORD = os.getenv("ARROW_PASSWORD", "")


def _require(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise SystemExit(f"missing {name} in .env - copy .env.example to .env and fill it in")
    return v


def create_client() -> tuple[ArrowClient, dict]:
    """Create and auto-login ArrowClient (TOTP handled from ARROW_TOTP_SECRET)."""
    client = ArrowClient(app_id=_require("ARROW_APP_ID"))
    resp = client.auto_login(
        user_id=_require("ARROW_USER_ID"),
        password=_require("ARROW_PASSWORD"),
        api_secret=_require("ARROW_APP_SECRET"),  # also accepts app_secret=
        totp_secret=_require("ARROW_TOTP_SECRET"),
    )
    return client, resp


def create_client_verbose():
    print(f"App ID: {APP_ID or '(not set)'}")
    print(f"User ID: {USER_ID or '(not set)'}")
    try:
        client, resp = create_client()
        print("Successfully logged in!")
        print("Response:", resp)
        token = client.get_token()
        print("Token:", token[:20] + "..." if token else "none")
        return client
    except Exception as e:
        print("Login Error:", e)
        raise


if __name__ == "__main__":
    create_client_verbose()
