"""
Entry point — works both locally and in Databricks Apps.
Loads .env for local dev; in deployed Apps the platform sets env vars directly.
"""
import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env from the same directory as this file (no-op if missing or if
# the vars are already set by the platform).
load_dotenv(Path(__file__).parent / ".env", override=False)

import uvicorn
from backend.main import app

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
