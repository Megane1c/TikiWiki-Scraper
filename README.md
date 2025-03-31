# Web Scraper for Tiki Knowledge Base Indexing

This repository contains a web scraper designed to extract content from a knowledge base that uses Tiki Wiki CMS and index it into Meilisearch for efficient search and retrieval.

## Features
- Extracts structured content from web pages (text, tables, lists, links)
- Indexes data into Meilisearch for fast searching
- Automatically detects and processes updated pages
- Multi-threaded processing for improved performance

## Requirements
### Software Dependencies
- Python 3.12
- Modules in **requirements.txt**
- Docker or Podman for running **Meilisearch**

### Environment
- A running instance of [Meilisearch](https://www.meilisearch.com/)
- An `.env` file containing:
  - `BASE_URL` 
  - `MEILISEARCH_URL` 
  - `ADMIN_KEY` for Meilisearch if you implemented the MASTER_KEY 
  - `OLLAMA_URL` to use Ollama's API for summarizing web content

## Installation
1. Clone the repository
2. Install dependencies:
   ```sh
   pip install -r requirements.txt
   ```
3. Configure your environment `.env`

## Usage
To run the scraper:
```sh
python scrape.py
```
The script will process updated pages and index them into Meilisearch.

## Meilisearch Integration
The scraper interacts with Meilisearch to:
- Create an index (if not exists)
- Add or update documents with extracted content
- Track indexing tasks for completion

Ensure Meilisearch is running before executing the script. Use Docker or Podman to run a Meilisearch instance.

## Logging
The script uses Python's `logging` module to track execution progress. Logs are printed to the console with timestamps.

