import json
import os
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import feedparser
from dateutil import parser as dateparser

# -----------------------------
# Paths
# -----------------------------
ROOT = Path(__file__).resolve().parents[1]
SOURCES_DIR = ROOT / "sources"
DATA_DIR = ROOT / "data"
DIGESTS_DIR = ROOT / "digests"

RSS_FILE = SOURCES_DIR / "rss_sources.txt"
SEED_URLS_FILE = SOURCES_DIR / "seed_urls.txt"
SEEN_FILE = DATA_DIR / "seen.json"

# -----------------------------
# Config
# -----------------------------
MAX_ITEMS_PER_FEED = int(os.getenv("MAX_ITEMS_PER_FEED", "10"))
DIGEST_TITLE = os.getenv("DIGEST_TITLE", "Service Design Digest")

# -----------------------------
# Helpers
# -----------------------------
def read_lines(path: Path) -> list[str]:
    if not path.exists():
        return []
    lines = []
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        lines.append(s)
    return lines


def load_seen() -> dict:
    if SEEN_FILE.exists():
        return json.loads(SEEN_FILE.read_text(encoding="utf-8"))
    return {"seen_urls": {}, "last_run_utc": None}


def save_seen(seen: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    SEEN_FILE.write_text(
        json.dumps(seen, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def normalize_url(url: str) -> str:
    try:
        parsed = urlparse(url)
        scheme = parsed.scheme.lower()
        netloc = parsed.netloc.lower()
        path = parsed.path or ""
        query = f"?{parsed.query}" if parsed.query else ""
        return f"{scheme}://{netloc}{path}{query}"
    except Exception:
        return url


def parse_entry_date(entry) -> datetime | None:
    for key in ("published", "updated", "created"):
        if entry.get(key):
            try:
                return dateparser.parse

