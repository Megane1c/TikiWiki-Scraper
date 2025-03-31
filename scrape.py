import requests
from bs4 import BeautifulSoup
import datetime
import urllib.parse
import time
from typing import Dict, Any, Optional, Tuple, List
import logging
import json
import urllib3
import os
import hashlib
from requests.adapters import HTTPAdapter
from utils.get_page_hash import detect_updated_pages
from dotenv import load_dotenv

load_dotenv()

# TIKI URLS
TIKI_URLS=detect_updated_pages()

# Disable SSL verification warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MeilisearchIndexer:
    def __init__(self, host: str = f"{os.getenv('MEILISEARCH_URL')}", index_name: str = "documents"):
        self.host = host
        self.index_name = index_name
        self.headers = {"Content-Type": "application/json", "Authorization": f"Bearer {os.getenv('ADMIN_KEY')}"}
        self.setup_index()

    def setup_index(self):
        try:
            response = requests.get(f"{self.host}/indexes/{self.index_name}", headers=self.headers, verify=False)
            if response.status_code == 404:
                create_response = requests.post(
                    f"{self.host}/indexes",
                    headers=self.headers,
                    json={"uid": self.index_name, "primaryKey": "id"},
                    verify=False
                )
                if create_response.status_code != 202:
                    raise Exception(f"Failed to create index: {create_response.text}")
                task_id = create_response.json()['taskUid']
                self._wait_for_task(task_id)
                logger.info("Index created successfully")
            else:
                logger.info("Index already exists")
        except Exception as e:
            logger.error(f"Error setting up index: {e}")
            raise

    def _wait_for_task(self, task_id: int, timeout: int = 60) -> bool:
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = requests.get(f"{self.host}/tasks/{task_id}", verify=False)
                if response.status_code == 200:
                    task = response.json()
                    if task['status'] == 'succeeded':
                        return True
                    elif task['status'] == 'failed':
                        logger.error(f"Task {task_id} failed: {task.get('error', {}).get('message', 'Unknown error')}")
                        return False
            except Exception as e:
                logger.error(f"Error checking task status: {e}")
            time.sleep(1)
        return False

    def index_document(self, doc_data: Dict[str, Any]) -> bool:
        try:
            response = requests.post(
                f"{self.host}/indexes/{self.index_name}/documents",
                headers=self.headers,
                json=[doc_data],
                verify=False
            )
            if response.status_code != 202:
                logger.error(f"Failed to index document: {response.text}")
                return False
            task_id = response.json()['taskUid']
            return self._wait_for_task(task_id)
        except Exception as e:
            logger.error(f"Error indexing document: {e}")
            return False

class WebScraper:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.indexer = MeilisearchIndexer()
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers.update({'User-Agent': 'Mozilla/5.0'})
        max_retries = urllib3.util.retry.Retry(
            total=10,
            backoff_factor=2,
            status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=max_retries)
        self.session.mount("https://", adapter)

    def normalize_url(self, url: str) -> Optional[str]:
        if url.startswith('//'):
            url = 'https:' + url
        return urllib.parse.urljoin(self.base_url, url)

    def is_valid_url(self, url: str) -> bool:
        if url.endswith('&display=pdf') or 'tiki-print.php' in url:
            return False
        parsed = urllib.parse.urlparse(url)
        return parsed.netloc and parsed.scheme in ('http', 'https')

    def generate_summary(self, title: str, content: str):
        query = f"""# Role: Context-Aware Webpage Summarizer
            Your task is to generate a concise, accurate summary of markdown-formatted content while **critically evaluating** whether the page title aligns with the content\n\n.

            **Webpage Content**:
            - Title: {title}
            - Content (markdown-like syntax): {content}

            \n\n# Rules
            1. **Summary Guidelines**:
            - Prioritize claims, definitions, processes, or unique insights.
            - For technical guides: Highlight steps/tools.
            - For policies: Extract rules/requirements.
            - **Keywords**: Include proper nouns, tools, or concepts (e.g., "Docker", "GDPR compliance").

            2. **Output Requirements**:
            - Return a JSON object with:
                ```json
                {{
                "summary": "1-2 sentences describing the page's purpose and primary topics.",
                "keywords": ["list", "of", "5-10", "key", "terms"],
                }}
                ```
            """
        payload = json.dumps({"model": "mistral-small:24b-instruct-2501-fp16", "temperature": 0, "messages": [{"role": "user", "content": query}], "stream": False })
        response = requests.post(f"{os.getenv('OLLAMA_URL')}", data=payload, headers={"Content-type": "application/json"})
        return json.loads(response.json()["message"]["content"].strip('\n').replace('```json', '').replace('```', '').strip())

    def _process_list_items(self, list_elem) -> List:
        """
        Recursively process list items, preserving the hierarchical structure.
        """
        items = []
        
        for li in list_elem.find_all('li', recursive=False):
            item_data = {
                "text": "",
                "links": [],
                "nested_list": None
            }
            
            # First, get all direct links in this list item (not in nested lists)
            direct_links = []
            nested_lists = li.find_all(['ul', 'ol'], recursive=False)
            
            # Create a temporary copy of the li to remove nested lists
            li_copy = BeautifulSoup(str(li), 'html.parser')
            # Remove nested lists from the copy
            for nested_list in li_copy.find_all(['ul', 'ol']):
                nested_list.decompose()
                
            # Now extract links from the modified copy (which has no nested lists)
            for link in li_copy.find_all('a', href=True):
                href = link.get('href')
                
                # Handle relative URLs
                if href and not href.startswith(('http://', 'https://')):
                    absolute_href = urllib.parse.urljoin(self.base_url, href)
                else:
                    absolute_href = href
                
                link_text = link.get_text(strip=True)
                if not link_text:
                    link_text = absolute_href
                    
                direct_links.append({
                    "text": link_text,
                    "href": absolute_href
                })
                
            # Get direct text of this li, excluding links and nested lists
            # Remove all links from li_copy
            for link in li_copy.find_all('a'):
                link.decompose()
                
            # Now get the remaining text
            item_data["text"] = li_copy.get_text(strip=True)
            item_data["links"] = direct_links
            
            # Process nested list separately to maintain hierarchy
            if nested_lists:
                for nested_list in nested_lists:
                    item_data["nested_list"] = {
                        "list_type": nested_list.name,
                        "items": self._process_list_items(nested_list)
                    }
                    break  # Just take the first nested list for now
            
            items.append(item_data)
        
        return items

    def format_nested_list_as_text(self, list_data: Dict, indent_level: int = 0) -> str:
        """
        Convert nested list data structure to a textual representation that preserves hierarchy.
        """
        text_parts = []
        indent = "  " * indent_level
        
        for idx, item in enumerate(list_data.get("items", [])):
            # Add list marker based on list type
            if list_data.get("list_type") == "ol":
                marker = f"{idx + 1}."
            else:
                marker = "•"
            
            # Add the item text
            if item.get('text'):
                text_parts.append(f"{indent}{marker} {item.get('text')}")
            else:
                text_parts.append(f"{indent}{marker}")
            
            # Add direct links at this level
            for link in item.get("links", []):
                link_text = f"{indent}  - [{link.get('text', '')}]({link.get('href', '')})"
                text_parts.append(link_text)
            
            # Process nested list with increased indentation to show hierarchy
            nested_list = item.get("nested_list")
            if nested_list:
                nested_text = self.format_nested_list_as_text(nested_list, indent_level + 1)
                text_parts.append(nested_text)
        
        return "\n".join(text_parts)

    def extract_table_data(self, soup_or_table) -> List[Dict]:
        """
        Extract data from tables in the HTML content.
        Returns a list of dictionaries, each representing a table with its structure.
        Properly handles relative links and nested lists within cells.
        """
        tables_data = []
        
        # Handle different input types
        if hasattr(soup_or_table, 'name') and soup_or_table.name == 'table':
            # Single table element
            tables = [soup_or_table]
        elif hasattr(soup_or_table, 'find_all'):
            # BeautifulSoup object
            tables = soup_or_table.find_all('table')
        else:
            # Unsupported type
            logger.warning(f"Unsupported type for extract_table_data: {type(soup_or_table)}")
            return []
        
        for table_idx, table in enumerate(tables):
            table_data = {
                "table_id": f"table_{table_idx}",
                "caption": "",
                "headers": [],
                "rows": []
            }
            
            # Extract caption if available
            caption = table.find('caption')
            if caption:
                table_data["caption"] = caption.get_text(strip=True)
            
            # Extract headers
            headers = []
            header_row = table.find('thead')
            if header_row:
                for th in header_row.find_all('th'):
                    headers.append(th.get_text(strip=True))
            else:
                # Try first tr as header if thead not found
                first_tr = table.find('tr')
                if first_tr:
                    for th in first_tr.find_all(['th', 'td']):
                        headers.append(th.get_text(strip=True))
            
            table_data["headers"] = headers
            
            # Extract rows
            rows = []
            tbody = table.find('tbody')
            if tbody:
                trs = tbody.find_all('tr')
                if not header_row and trs:
                    trs = trs[1:]
            else:
                # Get all rows if tbody not found
                trs = table.find_all('tr')
                # Skip the first row if we used it as headers
                if not header_row and trs:
                    trs = trs[1:]
            
            for tr in trs:
                row = []
                for td in tr.find_all(['td', 'th']):
                    # Handle cell content with potential lists and links
                    cell_content = {
                        "text": "",
                        "links": [],
                        "lists": []
                    }
                    
                    # Process links
                    links = td.find_all('a', href=True)
                    if links:
                        processed_links = []
                        for link in links:
                            href = link.get('href')
                            
                            # Handle relative URLs - convert to absolute
                            if href and not href.startswith(('http://', 'https://')):
                                absolute_href = urllib.parse.urljoin(self.base_url, href)
                            else:
                                absolute_href = href

                            span = link.find('span')
                            if span:
                                link_text = span.get_text(strip=True)
                            else:
                                link_text = link.get_text(strip=True)
                                
                            processed_links.append({
                                "text": link_text,
                                "href": absolute_href
                            })
                        
                        cell_content["links"] = processed_links
                    
                    # Process lists within the cell
                    lists = td.find_all(['ul', 'ol'])
                    if lists:
                        for lst in lists:
                            list_type = lst.name
                            list_items = [li.get_text(strip=True) for li in lst.find_all('li')]
                            cell_content["lists"].append({
                                "type": list_type,
                                "items": list_items
                            })
                            
                            # Remove lists from text content
                            for li in lst:
                                li.decompose()
                    
                    # Get remaining text content after removing lists and processing links
                    cell_text = td.get_text(strip=True)
                    cell_content["text"] = cell_text
                    
                    row.append(cell_content)
                
                if row:  # Only add non-empty rows
                    rows.append(row)
            
            table_data["rows"] = rows
            
            # Only add tables with actual data
            if headers or rows:
                tables_data.append(table_data)
        
        return tables_data

    def format_table_as_text(self, table_data: Dict) -> str:
        """
        Convert table data structure to a textual representation for indexing.
        Properly handles nested lists and cell contents.
        """
        text_parts = []
        
        if table_data.get("caption"):
            text_parts.append(f"Table: {table_data['caption']}")
        
        # Add headers
        if table_data.get("headers"):
            text_parts.append(" | ".join(table_data["headers"]))
        
        # Add rows
        for row in table_data.get("rows", []):
            row_text = []
            for cell in row:
                # Handle structured cell content
                cell_texts = []
                
                # Add main text
                if isinstance(cell, dict):
                    if cell.get("text"):
                        cell_texts.append(cell["text"])
                    
                    # Add links
                    for link in cell.get("links", []):
                        if cell.get("text") == link['text']:
                            cell_texts.append(f"({link['href']})")
                    
                    # Add lists
                    for lst in cell.get("lists", []):
                        list_marker = "•" if lst["type"] == "ul" else "+"
                        for item in lst["items"]:
                            # Check if the item has a corresponding link
                            linked_item = next((link for link in cell.get("links", []) if link["text"] == item), None)
                            if linked_item:
                                list_item = f"{list_marker} [{item}]({linked_item['href']})"
                            else:
                                list_item = f"{list_marker} {item}"
                            cell_texts.append(list_item)
                else:
                    cell_texts.append(str(cell))
                
                row_text.append(" ".join(cell_texts))
            
            text_parts.append(" | ".join(row_text))
        
        return "\n".join(text_parts)

    def extract_title_and_description_from_text(self, text: str) -> Tuple[str, str]:
        """
        Extracts title and description from a single text string.
        """
        text = text.strip()
        
        # Common separators
        separators = [' - ', ': ', ' | ', ' – ', ' — ']

        for separator in separators:
            if separator in text:
                parts = text.split(separator, 1)
                return parts[0].strip(), parts[1].strip()
        
        # Check for parenthetical description
        if ')' in text and '(' in text:
            start_idx = text.find('(')
            end_idx = text.rfind(')')
            if start_idx > 0:  # Ensure there's text before the parentheses
                title = text[:start_idx].strip()
                description = text[start_idx + 1:end_idx].strip()
                return title, description
        
        # If no separator is found, treat whole text as title
        return text, ""

    def get_link_description(self, link_element) -> Tuple[str, str]:
        """
        Extract both the link text and any nearby description text.
        Returns a tuple of (title, description)
        """
        # First try to extract title and description from the link text itself
        link_text = link_element.get_text(strip=True)
        title, description = self.extract_title_and_description_from_text(link_text)
        
        if description:
            return title, description
            
        # If no description was found in the link text, look for external description
        
        # Check if link is inside a <strong> tag with description
        parent_strong = link_element.find_parent('strong')
        if parent_strong:
            full_text = parent_strong.get_text(strip=True)
            external_desc = full_text.replace(link_text, '').strip()
            if external_desc:
                return title, external_desc

        # Check for <strong> tag after the link
        next_strong = link_element.find_next_sibling('strong')
        if next_strong:
            external_desc = next_strong.get_text(strip=True)
            if external_desc:
                return title, external_desc

        # Check for description in parent paragraph
        parent_p = link_element.find_parent('p')
        if parent_p:
            # Get text after the link while preserving strong tags
            try:
                link_index = parent_p.contents.index(link_element)
                description_elements = parent_p.contents[link_index + 1:]
                external_desc = ' '.join(
                    elem.get_text(strip=True) if hasattr(elem, 'get_text') 
                    else str(elem).strip()
                    for elem in description_elements 
                    if elem and (isinstance(elem, str) or elem.name in ['strong', 'span', 'em'])
                ).strip()
                if external_desc:
                    return title, external_desc
            except ValueError:
                pass  # Link not directly in paragraph contents
        
        # Check for description in parent list item
        parent_li = link_element.find_parent('li')
        if parent_li:
            full_text = parent_li.get_text(strip=True)
            external_desc = full_text.replace(link_text, '').strip()
            if external_desc:
                return title, external_desc
        
        # If no external description was found, return the original title and empty description
        return title, ""

    def extract_structured_content(self, soup) -> Dict:
        """
        Extract content from HTML while preserving the original document structure.
        Returns a dictionary with text content and structured data (tables, lists).
        """
        result = {
            "text": "",
            "tables": [],
            "lists": []
        }
        
        # If there's no content, extract basic text and return
        if not soup or not hasattr(soup, 'find_all'):
            if soup:
                result["text"] = str(soup) if isinstance(soup, str) else soup.get_text(strip=True)
            return result
        
        # Process elements in their original order
        content_parts = []
        current_position = 0
        table_counter = 0
        list_counter = 0
        
        # Track root-level lists to avoid processing nested lists separately
        processed_list_roots = set()
        
        # Walk through all top-level elements in original order
        for element in soup.children:
            if not hasattr(element, 'name'):
                # Handle text nodes if they contain non-whitespace
                text = element.strip()
                if text:
                    content_parts.append({
                        "position": current_position,
                        "type": "text",
                        "content": text
                    })
                    current_position += 1
                continue
                
            # Process regular content (paragraphs, headings)
            if element.name in ['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                link = element.find('a', href=True)
                if link:
                    # Normalize URL
                    href = link.get('href')
                    if href and not href.startswith(('http://', 'https://')):
                        abs_href = urllib.parse.urljoin(self.base_url, href)
                    else:
                        abs_href = href

                    content_parts.append({
                    "position": current_position,
                    "type": "text",
                    "content": f"[{element.get_text(strip=True)}]({abs_href})"
                    })

                else:
                    content_parts.append({
                        "position": current_position,
                        "type": "text",
                        "content": element.get_text(strip=True)
                    })

                current_position += 1
                
            # Process tables
            elif element.name == 'table':
                tables = self.extract_table_data(element)
                table_data = tables[0] if tables else None
                if table_data:
                    table_id = f"table_{table_counter}"
                    table_counter += 1
                    
                    # Add table to structured data
                    table_data["table_id"] = table_id
                    result["tables"].append(table_data)
                    
                    # Add table placeholder in content flow
                    content_parts.append({
                        "position": current_position,
                        "type": "table",
                        "id": table_id,
                        "text": self.format_table_as_text(table_data)
                    })
                    current_position += 1
                    
            # Process lists - only root lists, not nested ones
            elif element.name in ['ul', 'ol'] and element not in processed_list_roots:
                # Check if this list is nested within another list
                parent_list = element.find_parent(['ul', 'ol'])
                if parent_list is None:  # Only process root lists
                    list_data = {
                        "list_id": f"list_{list_counter}",
                        "list_type": element.name,
                        "items": self._process_list_items(element)
                    }
                    list_counter += 1
                    
                    # Mark this list as processed
                    processed_list_roots.add(element)
                    
                    # Add list to structured data if it has items
                    if list_data["items"]:
                        result["lists"].append(list_data)
                        
                        # Add list placeholder in content flow
                        content_parts.append({
                            "position": current_position,
                            "type": "list",
                            "id": list_data["list_id"],
                            "text": self.format_nested_list_as_text(list_data)
                        })
                        current_position += 1
                    
            # Process divs that might contain content
            elif element.name == 'div':
                # Recursively process div contents
                nested_content = self.extract_nested_div_content(element, processed_list_roots)
                if nested_content:
                    for item in nested_content:
                        if item["type"] == "table":
                            table_id = f"table_{table_counter}"
                            table_counter += 1
                            item["table_data"]["table_id"] = table_id
                            result["tables"].append(item["table_data"])
                            content_parts.append({
                                "position": current_position,
                                "type": "table",
                                "id": table_id,
                                "text": self.format_table_as_text(item["table_data"])
                            })
                        elif item["type"] == "list":
                            list_id = f"list_{list_counter}" 
                            list_counter += 1
                            item["list_data"]["list_id"] = list_id
                            result["lists"].append(item["list_data"])
                            content_parts.append({
                                "position": current_position,
                                "type": "list",
                                "id": list_id,
                                "text": self.format_nested_list_as_text(item["list_data"])
                            })
                        else:
                            content_parts.append({
                                "position": current_position,
                                "type": "text",
                                "content": item["content"]
                            })
                        current_position += 1
        
        # If no structured content was found, extract all text content
        if not content_parts:
            result["text"] = soup.get_text(strip=True)
            return result
        
        # Sort content parts by their position to maintain original document flow
        content_parts.sort(key=lambda x: x["position"])
        
        # Combine all content parts in order
        full_content = []
        for part in content_parts:
            if part["type"] == "text":
                if part["content"].strip():
                    full_content.append(part["content"])
            else:  # table or list
                full_content.append(part["text"])
                
        result["text"] = "\n\n".join(full_content)
        return result

    def extract_nested_div_content(self, div_element, processed_list_roots) -> List[Dict]:
        """
        Process content within a div element, maintaining order of tables, lists, and text.
        """
        content_items = []
        
        for element in div_element.children:
            if not hasattr(element, 'name'):
                # Handle text node if it's not just whitespace
                text = element.strip()
                if text:
                    content_items.append({
                        "type": "text",
                        "content": text
                    })
                continue
                
            # Process paragraphs and headings
            if element.name in ['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                text = element.get_text(strip=True)
                if text:
                    content_items.append({
                        "type": "text",
                        "content": text
                    })
                    
            # Process tables
            elif element.name == 'table':
                tables = self.extract_table_data(element)
                table_data = tables[0] if tables else None
                if table_data:
                    content_items.append({
                        "type": "table",
                        "table_data": table_data
                    })
                    
            # Process lists - only root lists, not nested ones
            elif element.name in ['ul', 'ol'] and element not in processed_list_roots:
                # Check if this list is nested within another list
                parent_list = element.find_parent(['ul', 'ol'])
                if parent_list is None:  # Only process root lists
                    list_data = {
                        "list_type": element.name,
                        "items": self._process_list_items(element)
                    }
                    # Mark this list as processed
                    processed_list_roots.add(element)
                    
                    if list_data["items"]:
                        content_items.append({
                            "type": "list",
                            "list_data": list_data
                        })
                    
            # Recursively process nested divs
            elif element.name == 'div':
                nested_items = self.extract_nested_div_content(element, processed_list_roots)
                if nested_items:
                    content_items.extend(nested_items)
        
        return content_items

    def process_page(self, url: str) -> None:
        try:
            logger.info(f"Processing: {url}")
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            page_title = soup.title.string.strip() if soup.title else ""
            page_content = soup.find('div', id='page-data')
                
            # Extract content preserving the original structure
            structured_content = self.extract_structured_content(page_content)

            content_summary = self.generate_summary(page_title if "page=" in url else "", structured_content["text"])
            
            # Index the main page content
            doc_data = {
                "id": hashlib.md5(url.encode()).hexdigest(),
                "url": url,
                "title": page_title or url,
                "content": structured_content["text"],
                "summary": content_summary.get("summary"),
                "keywords": ", ".join(content_summary.get("keywords")),
            }

            # Index update time
            doc_data["indexed_at"]: datetime.datetime.now().isoformat()
            
            # Check for existing document
            search_response = requests.post(
                f"{self.indexer.host}/indexes/{self.indexer.index_name}/search",
                headers=self.indexer.headers,
                json={"q": hashlib.md5(url.encode()).hexdigest()},
                verify=False
            )

            existing_doc = None
            if search_response.status_code == 200:
                results = search_response.json().get('hits', [])
                if results and results[0].get('url') == url:
                    existing_doc = results[0]

            if existing_doc:
                logger.info(f"Updating webpage: {url}")
                doc_data["id"] = existing_doc["id"]
                update_response = requests.put(
                    f"{self.indexer.host}/indexes/{self.indexer.index_name}/documents",
                    headers=self.indexer.headers,
                    json=[doc_data],
                    verify=False
                )
                if update_response.status_code != 202:
                    logger.error(f"Failed to update document: {update_response.text}")
            else:
                self.indexer.index_document(doc_data)

        except Exception as e:
            logger.error(f"Error processing {url}: {e}")

    def start(self):
        try:
            logger.info("Starting to process URLs from TIKI_URLS list")
            for url in TIKI_URLS:
                self.process_page(url)
            logger.info("Processing completed.")
        except Exception as e:
            logger.error(f"Fatal error during processing: {e}")


def main():
    try:
        if len(TIKI_URLS) == 0:
            logger.info("No changes detected in Tiki Knowledge Base")
            import sys
            sys.exit(0)
        scraper = WebScraper(f"{os.getenv('BASE_URL')}")
        scraper.start()
    except Exception as e:
        logger.error(f"Script failed: {e}")

if __name__ == "__main__":
    main()
