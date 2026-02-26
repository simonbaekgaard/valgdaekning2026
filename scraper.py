import httpx
import gzip
import time
import random
import logging
import os
from datetime import datetime
import psycopg2

# --- Configuration ---
SITES = [
    "https://politiken.dk/",
    "https://tv2.dk/",
    "https://www.dr.dk/",
    "https://www.berlingske.dk/",
    "https://jyllands-posten.dk/",
    "https://www.information.dk/",
    "https://www.kristeligt-dagblad.dk/",
    "https://ekstrabladet.dk/",
    "https://www.bt.dk/",
    "https://www.weekendavisen.dk/",
]

DELAY_BETWEEN_REQUESTS = (2, 5)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "da,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)


# --- Database ---
def get_conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])


def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS scrapes (
                    id          SERIAL PRIMARY KEY,
                    url         TEXT NOT NULL,
                    scraped_at  TIMESTAMPTZ NOT NULL,
                    status_code INTEGER,
                    html        BYTEA,
                    error       TEXT
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_url_time ON scrapes (url, scraped_at)
            """)
    log.info("Database ready")


def save_scrape(url, scraped_at, status_code=None, html=None, error=None):
    compressed = gzip.compress(html.encode("utf-8")) if html else None
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO scrapes (url, scraped_at, status_code, html, error) VALUES (%s, %s, %s, %s, %s)",
                (url, scraped_at, status_code, compressed, error),
            )
    log.info(f"  Saved: {url}")


# --- Scraping ---
def scrape_site(url: str) -> dict:
    scraped_at = datetime.utcnow().isoformat()
    try:
        response = httpx.get(url, headers=HEADERS, timeout=15, follow_redirects=True)
        log.info(f"✓ {url} — {response.status_code} ({len(response.text):,} chars)")
        return {
            "url": url,
            "scraped_at": scraped_at,
            "status_code": response.status_code,
            "html": response.text,
            "error": None,
        }
    except Exception as e:
        log.warning(f"✗ {url} — {e}")
        return {
            "url": url,
            "scraped_at": scraped_at,
            "status_code": None,
            "html": None,
            "error": str(e),
        }


def run_scrape_round():
    log.info(f"--- Starting scrape round ({len(SITES)} sites) ---")
    for url in SITES:
        result = scrape_site(url)
        save_scrape(**result)
        delay = random.uniform(*DELAY_BETWEEN_REQUESTS)
        time.sleep(delay)
    log.info("--- Round complete ---")


if __name__ == "__main__":
    init_db()
    run_scrape_round()
