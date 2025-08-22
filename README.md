# BeautifulSoup Web Scraping Pipeline

A modular, production-style data pipeline built with **Python, Requests, and BeautifulSoup** for scraping and structuring company licensee data.

---

## Data Model & Schema Design
Each record extracted from the website is normalized into JSON:

```json
{
  "company": "ABC Capital Ltd.",
  "license_type": "Type 1 - Securities",
  "status": "Active"
}

// company → String (Company name)

// license_type → String (Regulatory license type)

// status → String (Current license status)


"""
Decisions & Trade-offs

BeautifulSoup over Firecrawl

Simple, lightweight, great for structured pages

Struggles if site uses heavy JavaScript rendering

JSON storage chosen

Human-readable, easy to extend to CSV/DB later

Not optimized for large-scale analytics

Pipeline modularity

Separated into ingestion → transform → storage for maintainability

Easier to swap ingestion logic (e.g., switch to Selenium for JS-heavy sites)
"""



"""
// How to Deploy & Run

// Clone the repository

git clone https://github.com/neo14166/SFC-Licensees-on-Webb-site---web-scrappin-using-beautifulsoup

// Install dependencies
pip install -r requirements.txt

// Run the pipeline
python main.py

// Check output
cat output.json
"""