import requests
from bs4 import BeautifulSoup
import time
import urllib3
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from dotenv import load_dotenv

load_dotenv()

# Disable SSL verification warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SESSION = requests.Session()
SESSION.verify = False
retries = Retry(total=10, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
SESSION.mount('https://', HTTPAdapter(max_retries=retries))
MAX_PER_PAGE = 300
OFFSET = 0
all_links = []

HEADERS = {
    "X-Requested-With": "XMLHttpRequest"  # AJAX request
}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
tiki_link = os.path.join(BASE_DIR, "tiki_pages")
os.makedirs(tiki_link, exist_ok=True)

# File paths for caching results
VALID_FILE = os.path.join(tiki_link, "valid_links.txt")
INVALID_FILE = os.path.join(tiki_link, "invalid_links.txt")

# Load existing valid and invalid links
def load_checked_links():
    """Loads previously checked valid and invalid links from files."""
    try:
        with open(VALID_FILE, "r") as f:
            valid_links = set(f.read().splitlines())
    except FileNotFoundError:
        valid_links = set()
    
    try:
        with open(INVALID_FILE, "r") as f:
            invalid_links = set(f.read().splitlines())
    except FileNotFoundError:
        invalid_links = set()
    
    return valid_links, invalid_links

# Save links to their respective files
def save_links(file, links):
    """Appends new links to a file."""
    with open(file, "a") as f:
        f.write("\n".join(links) + "\n")

def fetch_page_links(offset):
    """Fetch AJAX-loaded page links from tiki-listpages.php."""
    params = {
        "maxRecords": MAX_PER_PAGE,
        "offset": offset,
        "tsAjax": "y",
        "sort_mode": "pageName_asc"
    }
    
    response = SESSION.get(f"{os.getenv('TIKI_PAGES')}", params=params, headers=HEADERS)
    
    if response.status_code != 200:
        print(f"Failed to fetch page at offset {offset}: {response.status_code}")
        return []
    
    soup = BeautifulSoup(response.text, "html.parser")
    links = [f"https://kbase.asti.dost.gov.ph/{a['href']}" 
             for a in soup.select("a[href*='tiki-index.php?page']")]
    
    return links

def check_url_status(url):
    """Check if a URL is valid (HTTP 200) or broken (HTTP 404)."""
    try:
        response = SESSION.get(url)
        return url, response.status_code == 200
    except SESSION.RequestException:
        return url, False


# Load previously checked links
valid_links, invalid_links = load_checked_links()

# Fetch all page links
while True:
    print(f"Fetching pages with offset {OFFSET}...")
    page_links = fetch_page_links(OFFSET)
    
    if not page_links:
        break
    
    all_links.extend(page_links)
    OFFSET += MAX_PER_PAGE
    time.sleep(1)

# Filter links already checked
new_links = [url for url in all_links if url not in valid_links and url not in invalid_links]

print(f"Total links fetched: {len(all_links)}")
print(f"New links to check: {len(new_links)}")

# Parallelized link checking
if len(new_links) != 0:
    checked_valid = []
    checked_invalid = []

    with ThreadPoolExecutor(max_workers=6) as executor:
        future_to_url = {executor.submit(check_url_status, link): link for link in new_links}

        for future in as_completed(future_to_url):
            url = future_to_url[future]
            print(f"Checking: {url}", flush=True)

            try:
                url, is_valid = future.result(timeout=10)
                print(f"Checked: {url} -> {'Valid' if is_valid else 'Invalid'}", flush=True)
                
                if is_valid:
                    checked_valid.append(url)
                else:
                    checked_invalid.append(url)
            except Exception as e:
                print(f"Error checking {url}: {e}", flush=True)

    # Save newly checked URLs
    if checked_valid:
        save_links(VALID_FILE, checked_valid)
    if checked_invalid:
        save_links(INVALID_FILE, checked_invalid)

    valid_links.update(checked_valid)  # Add new valid URLs

def valid_urls():
    # List of new valid URLs
    return list(valid_links)



