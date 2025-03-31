# Utilities for Web Scraper

This directory contains utility scripts used in the web scraper. These scripts help detect updated pages and retrieve valid URLs from the knowledge base.

## 1. `get_page_hash.py`

### Description
This script detects updated pages in the knowledge base by comparing content hashes. It fetches raw page content, generates a SHA-256 hash, and compares it with stored values. If a change is detected, the URL is marked for processing.

### Features
- Retrieves the latest raw content of pages
- Computes SHA-256 hash for comparison
- Maintains a cache of known hashes
- Parallelized processing for speed
- Uses retry logic to handle network failures

### How It Works
1. Loads cached hashes from `tiki_page_cache.json`
2. Fetches raw content of pages and calculates new hashes
3. Compares new hashes with stored values
4. If a page has changed, adds it to the updated list
5. Saves the updated cache
6. Returns a list of modified URLs

### Usage
```python
from get_page_hash import detect_updated_pages
updated_pages = detect_updated_pages()
print(updated_pages)  # List of changed URLs
```

---

## 2. `get_page_links.py`

### Description
This script retrieves all valid URLs from the knowledge base by scraping its listing page (`tiki-listpages.php`). It also verifies the status of each URL before marking it as valid.

### Features
- Scrapes pages using AJAX requests
- Retrieves URLs dynamically using pagination
- Validates each URL to filter out broken links
- Caches valid and invalid links for efficiency
- Uses parallel processing to check URL status

### How It Works
1. Fetches paginated lists of pages from the knowledge base
2. Extracts links from the listing page
3. Checks if each link is valid (HTTP 200) or broken (HTTP 404)
4. Stores valid and invalid links in cache files
5. Returns only valid links for further processing

### Usage
```python
from get_page_links import get_valid_urls
valid_urls = get_valid_urls()
print(valid_urls)  # List of valid Tiki KBase URLs
```

## Notes
- These scripts are crucial for optimizing the scraper by focusing only on updated and valid pages.
- Running `pages.py` first ensures that `kbase_hash.py` only checks valid URLs.
- Both scripts handle SSL verification warnings and network failures using retry logic.


