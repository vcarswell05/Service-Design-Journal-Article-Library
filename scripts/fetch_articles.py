import json
import os
import time
import socket
import http.client
from urllib.error import URLError
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import feedparser
from dateutil import parser as dateparser
socket.setdefaulttimeout(20)
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
    lines: list[str] = []
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
    SEEN_FILE.write_text(json.dumps(seen, indent=2, sort_keys=True), encoding="utf-8")


def normalize_url(url: str) -> str:
    try:
        parsed = urlparse(url)
        scheme = (parsed.scheme or "https").lower()
        netloc = parsed.netloc.lower()
        path = parsed.path or ""
        query = f"?{parsed.query}" if parsed.query else ""
        return f"{scheme}://{netloc}{path}{query}"
    except Exception:
        return url


def parse_entry_date(entry) -> datetime | None:
    for key in ("published", "updated", "created"):
        val = entry.get(key)
        if not val:
            continue
        try:
            dt = dateparser.parse(val)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            pass
    return None


def host_label(url: str) -> str:
    try:
        return urlparse(url).netloc.replace("www.", "")
    except Exception:
        return "source"


# -----------------------------
# Fetchers
# -----------------------------
def fetch_from_rss(rss_urls):
    items = []

    for feed_url in rss_urls:
        print(f"Fetching RSS: {feed_url}")

        # retry up to 2 times total
        for attempt in range(2):
            try:
                feed = feedparser.parse(feed_url)

                # if the feed returns an HTTP error status, treat it as a failure
                status = getattr(feed, "status", None)
                if status and status >= 400:
                    raise RuntimeError(f"HTTP {status}")

                # bozo means the feed had issues (sometimes still usable)
                if getattr(feed, "bozo", 0):
                    print(f"Warning: bozo feed for {feed_url}: {feed.bozo_exception}")

                items.extend(feed.entries or [])
                break  # success, stop retrying

            except (http.client.RemoteDisconnected, URLError, TimeoutError, socket.timeout, RuntimeError) as e:
                if attempt == 0:
                    print(f"Retrying RSS ({feed_url}) after error: {e}")
                    time.sleep(2)
                    continue

                print(f"Skipping RSS ({feed_url}) after repeated error: {e}")
                break  # skip this feed, move to next

            import ssl

...

except (
    http.client.RemoteDisconnected,
    http.client.IncompleteRead,
    ConnectionResetError,
    TimeoutError,
    socket.timeout,
    ssl.SSLError,
    URLError,
    OSError,
    RuntimeError,
) as e:


    return items


def fetch_from_seed_urls(seed_urls: list[str]) -> list[dict]:
    # No scraping; just include the URLs as items.
    items: list[dict] = []
    for url in seed_urls:
        items.append(
            {
                "title": url,
                "url": url,
                "source": host_label(url),
                "published_utc": None,
            }
        )
    return items


# -----------------------------
# Processing
# -----------------------------
def dedupe(items: list[dict], seen: dict) -> tuple[list[dict], dict]:
    seen_urls: dict = seen.get("seen_urls", {})
    new_items: list[dict] = []

    for item in items:
        normalized = normalize_url(item["url"])
        if normalized in seen_urls:
            continue

        seen_urls[normalized] = {
            "title": item["title"],
            "source": item["source"],
            "first_seen_utc": datetime.now(timezone.utc).isoformat(),
        }
        item["url"] = normalized
        new_items.append(item)

    seen["seen_urls"] = seen_urls
    seen["last_run_utc"] = datetime.now(timezone.utc).isoformat()
    return new_items, seen


def sort_items(items: list[dict]) -> list[dict]:
    def sort_key(item):
        pub = item.get("published_utc")
        if pub:
            try:
                return dateparser.parse(pub)
            except Exception:
                pass
        return datetime(1970, 1, 1, tzinfo=timezone.utc)

    return sorted(items, key=sort_key, reverse=True)


# -----------------------------
# Output
# -----------------------------
def write_digest(items: list[dict]) -> None:
    DIGESTS_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).date().isoformat()
    out_file = DIGESTS_DIR / f"{today}.md"

    lines: list[str] = []
    lines.append(f"# {DIGEST_TITLE} — {today}")
    lines.append("")
    lines.append(f"New items this run: **{len(items)}**")
    lines.append("")

    if not items:
        lines.append("_No new articles found._")
    else:
        grouped: dict[str, list[dict]] = {}
        for item in items:
            grouped.setdefault(item["source"], []).append(item)

        for source in sorted(grouped.keys(), key=str.lower):
            lines.append(f"## {source}")
            lines.append("")
            for item in sort_items(grouped[source]):
                title = item["title"]
                url = item["url"]
                pub = item.get("published_utc")
                if pub:
                    try:
                        date_str = dateparser.parse(pub).date().isoformat()
                        lines.append(f"- [{title}]({url}) ({date_str})")
                    except Exception:
                        lines.append(f"- [{title}]({url})")
                else:
                    lines.append(f"- [{title}]({url})")
            lines.append("")

    out_file.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


# -----------------------------
# Main (with debug prints)
# -----------------------------
def main():
    rss_urls = read_lines(RSS_FILE)
    seed_urls = read_lines(SEED_URLS_FILE)

    print(f"RSS_FILE path: {RSS_FILE}")
    print(f"SEED_URLS_FILE path: {SEED_URLS_FILE}")
    print(f"Loaded RSS feeds: {len(rss_urls)}")
    print(f"Loaded seed URLs: {len(seed_urls)}")

    if len(rss_urls) == 0 and len(seed_urls) == 0:
        raise SystemExit(
            "No sources loaded. Ensure sources/rss_sources.txt and/or sources/seed_urls.txt contain URLs."
        )

    seen = load_seen()
    before_seen = len(seen.get("seen_urls", {}))

try:
    rss_items = fetch_from_rss(rss_urls)
except Exception as e:
    print(f"RSS fetch failed unexpectedly; continuing with empty RSS set: {repr(e)}")
    rss_items = []
    seed_items = fetch_from_seed_urls(seed_urls)
    items = rss_items + seed_items

    print(f"Fetched items from RSS: {len(rss_items)}")
    print(f"Fetched items from seed URLs: {len(seed_items)}")
    print(f"Total fetched items: {len(items)}")

    new_items, seen = dedupe(items, seen)
    after_seen = len(seen.get("seen_urls", {}))

    print(f"Seen before: {before_seen} | Seen after: {after_seen}")
    print(f"New items this run: {len(new_items)}")

    save_seen(seen)
    write_digest(new_items)
    print("Digest written successfully.")


if __name__ == "__main__":
    main()


