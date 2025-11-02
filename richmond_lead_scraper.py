import os
import requests
from bs4 import BeautifulSoup
import re
import json
import time
import pandas as pd
import urllib.parse
from urllib.robotparser import RobotFileParser
from collections import defaultdict
from flask import Flask

# --------------------------------------------------------
# Secure Brevo API key
# --------------------------------------------------------
BREVO_API_KEY = os.getenv("BREVO_API_KEY")
if not BREVO_API_KEY:
    raise ValueError("Missing BREVO_API_KEY environment variable.")

# --------------------------------------------------------
# CONFIGURATION
# --------------------------------------------------------
CITY = "Richmond VA"
SEARCH_TERMS = [
    "new small businesses near " + CITY,
    "recently opened businesses in " + CITY,
    "local startups in " + CITY,
    "new companies in " + CITY,
    "new restaurants or services in " + CITY,
    "gyms in " + CITY,
    "coffee shops in " + CITY,
    "boutiques in " + CITY,
    "marketing agencies in " + CITY,
    "salons in " + CITY,
    "HVAC companies in " + CITY,
    "plumbers in " + CITY
]
HEADERS = {"User-Agent": "Mozilla/5.0"}

EMAIL_PATTERN = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_PATTERN = re.compile(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}")
NAME_KEYWORDS = ["owner", "manager", "founder", "ceo", "director"]

# --------------------------------------------------------
# POLITE SCRAPING UTILITIES
# --------------------------------------------------------
GLOBAL_MIN_DELAY = 2.0
PER_DOMAIN_MIN_DELAY = 10.0
MAX_REQUESTS_PER_RUN = 250
MAX_ATTEMPTS = 3

_last_request_time = 0.0
_domain_last_time = defaultdict(lambda: 0.0)
_robot_parsers = {}

def can_fetch_url(url, user_agent="*"):
    parsed = urllib.parse.urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    if base not in _robot_parsers:
        rp = RobotFileParser()
        try:
            rp.set_url(base + "/robots.txt")
            rp.read()
        except Exception:
            rp = None
        _robot_parsers[base] = rp
    rp = _robot_parsers.get(base)
    if not rp:
        return True
    return rp.can_fetch(user_agent, url)

def polite_get(url, headers=None, timeout=10):
    global _last_request_time
    parsed = urllib.parse.urlparse(url)
    domain = parsed.netloc.lower()
    if not url.startswith("http"):
        raise ValueError("Skipping non-http url")
    if not can_fetch_url(url, user_agent=HEADERS.get("User-Agent", "*")):
        raise RuntimeError(f"Disallowed by robots.txt: {url}")

    now = time.time()
    since_domain = now - _domain_last_time[domain]
    if since_domain < PER_DOMAIN_MIN_DELAY:
        time.sleep(PER_DOMAIN_MIN_DELAY - since_domain)
    now = time.time()
    since_global = now - _last_request_time
    if since_global < GLOBAL_MIN_DELAY:
        time.sleep(GLOBAL_MIN_DELAY - since_global)

    attempt = 0
    backoff = 1.5
    while attempt < MAX_ATTEMPTS:
        attempt += 1
        try:
            r = requests.get(url, headers=headers or HEADERS, timeout=timeout)
            _last_request_time = time.time()
            _domain_last_time[domain] = _last_request_time
            if r.status_code == 429:
                time.sleep(backoff * attempt)
                continue
            if r.status_code >= 500:
                time.sleep(backoff * attempt)
                continue
            return r
        except requests.RequestException:
            time.sleep(backoff * attempt)
    raise RuntimeError(f"Failed to fetch {url} after {MAX_ATTEMPTS} attempts")

# --------------------------------------------------------
# SCRAPER LOGIC
# --------------------------------------------------------
def search_businesses(term):
    print(f"\nSearching Google for: {term}")
    url = f"https://www.google.com/search?q={term}"
    response = polite_get(url, headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")
    links = []
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if href.startswith("http") and "google" not in href:
            links.append(href)
    links = [l for l in links if not any(bad in l for bad in ["yelp", "facebook", "tripadvisor", "bbb.org", "linkedin"])]
    return list(set(links))[:25]

def scrape_business_info(site_url):
    try:
        response = polite_get(site_url, headers=HEADERS, timeout=8)
        soup = BeautifulSoup(response.text, "html.parser")
        text = soup.get_text(" ", strip=True)
        emails = list(set(re.findall(EMAIL_PATTERN, text)))
        phones = list(set(re.findall(PHONE_PATTERN, text)))

        owner_name = ""
        for line in text.splitlines():
            if any(word in line.lower() for word in NAME_KEYWORDS):
                owner_name = line.strip()[:60]
                break

        business_name = soup.title.string.strip() if soup.title else "Unknown Business"
        return {
            "business_name": business_name,
            "owner_name": owner_name,
            "emails": emails,
            "phones": phones,
            "website": site_url
        }
    except Exception as e:
        print(f"Error scraping {site_url}: {e}")
        return None

def add_to_brevo(contact):
    if not contact["emails"]:
        return
    url = "https://api.brevo.com/v3/contacts"
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "api-key": BREVO_API_KEY
    }
    data = {
        "email": contact["emails"][0],
        "attributes": {
            "FIRSTNAME": contact.get("owner_name", ""),
            "COMPANY": contact.get("business_name", ""),
            "PHONE": contact["phones"][0] if contact["phones"] else "",
            "WEBSITE": contact["website"]
        },
        "listIds": [3]
    }
    r = requests.post(url, headers=headers, data=json.dumps(data))
    print(f"Added to Brevo: {contact['emails'][0]} ({r.status_code})")

# --------------------------------------------------------
# MAIN SCRAPER LOOP
# --------------------------------------------------------
def main():
    print("🔍 Starting Richmond Business Lead Scraper...")
    leads_df = pd.DataFrame(columns=["Business Name", "Owner Name", "Email", "Phone", "Website"])
    uploaded_count = 0
    max_leads = 25
    total_requests = 0

    for term in SEARCH_TERMS:
        if uploaded_count >= max_leads or total_requests >= MAX_REQUESTS_PER_RUN:
            break
        sites = search_businesses(term)
        for site in sites:
            if uploaded_count >= max_leads or total_requests >= MAX_REQUESTS_PER_RUN:
                break
            total_requests += 1
            info = scrape_business_info(site)
            if info and info["emails"]:
                if not any(leads_df["Email"].eq(info["emails"][0])):
                    leads_df.loc[len(leads_df)] = [
                        info["business_name"],
                        info["owner_name"],
                        info["emails"][0],
                        info["phones"][0] if info["phones"] else "",
                        info["website"]
                    ]
                    add_to_brevo(info)
                    uploaded_count += 1
                    print(f"✅ Added {info['emails'][0]} ({uploaded_count}/{max_leads})")
    print(f"\n🎯 Finished run. Uploaded {uploaded_count} leads to Brevo. Requests used: {total_requests}")

# --------------------------------------------------------
# FLASK SERVER
# --------------------------------------------------------
app = Flask(__name__)

@app.route("/")
def home():
    return "Richmond Lead Scraper is running."

@app.route("/run")
def run_scraper():
    print("🚀 /run endpoint triggered — scraper starting...")
    main()
    print("✅ Scraper completed successfully.")
    return "Scraper completed successfully."

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)




