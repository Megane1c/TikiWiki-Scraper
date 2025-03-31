import requests
import hashlib
import json
import concurrent.futures
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from .get_page_links import valid_urls
import urllib3

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

KBASE_URLS=valid_urls()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Store content hash
CACHE_FILE = os.path.join(BASE_DIR, "tiki_page_cache.json")

# Configure session with retry logic
session = requests.Session()
retries = Retry(
    total=10,
    backoff_factor=1,  
    status_forcelist=[500, 502, 503, 504],  # Server errors
)
session.mount("https://", HTTPAdapter(max_retries=retries))

def load_cache():
    """Load cached hashes from a JSON file."""
    try:
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def save_cache(cache):
    """Save the updated content hashes to a JSON file."""
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=4)

def convert_to_raw_url(url):
    return url.replace("tiki-index.php", "tiki-index_raw.php")

def fetch_and_check(url, cache):
    raw_url = convert_to_raw_url(url)

    try:
        response = session.get(raw_url, timeout=10, verify=False)
        response.raise_for_status()  # Raise error for bad responses (4xx, 5xx)
    except requests.RequestException as e:
        print(f"Failed to fetch {raw_url}: {e}")
        return None

    content = response.text
    new_hash = hashlib.sha256(content.encode()).hexdigest()
    old_hash = cache.get(url, {}).get("hash")  # Use original URL as key in cache

    if new_hash != old_hash:
        print(f"Change detected in {url}")
        cache[url] = {"hash": new_hash}  # Store hash under original URL
        return url

    return None  # No detected changes

def detect_updated_pages():
    """Parallelized function."""
    cache = load_cache()
    updated_pages = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        results = executor.map(lambda url: fetch_and_check(url, cache), KBASE_URLS)
        updated_pages = [url for url in results if url is not None]

    save_cache(cache)  # Save updated cache
    return updated_pages

