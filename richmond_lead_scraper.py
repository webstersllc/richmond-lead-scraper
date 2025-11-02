import os
import requests
from bs4 import BeautifulSoup
import re
import json
import time
import pandas as pd

# Get Brevo API key securely
BREVO_API_KEY = os.getenv("BREVO_API_KEY")
if not BREVO_API_KEY:
    raise ValueError("Missing BREVO_API_KEY environment variable. Set it before running this script.")

# --- SETTINGS ---
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

# --- Helper Functions ---
def search_businesses(term):
    print(f"\nSearching Google for: {term}")
    url = f"https://www.google.com/search?q={term}"
    response = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")
    links = []
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if "http" in href and "google" not in href:
            links.append(href)
    return list(set(links))[:15]

def scrape_business_info(site_url):
    try:
        response = requests.get(site_url, headers=HEADERS, timeout=8)
        soup = BeautifulSoup(response.text, "html.parser")
        text = soup.get_text(" ", strip=True)

        emails = list(set(re.findall(EMAIL_PATTERN, text)))
        phones = list(set(re.findall(PHONE_PATTERN, text)))

        # Find a possible owner/manager mention
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

# --- Main Script ---
def main():
    print("🔍 Starting Richmond Business Lead Scraper...")
    leads_df = pd.DataFrame(columns=["Business Name", "Owner Name", "Email", "Phone", "Website"])
    uploaded_count = 0
    max_leads = 25

    for term in SEARCH_TERMS:
        if uploaded_count >= max_leads:
            break
        sites = search_businesses(term)
        for site in sites:
            if uploaded_count >= max_leads:
                break

            info = scrape_business_info(site)
            if info and info["emails"]:
                # Check for duplicates in memory
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

                    time.sleep(2)  # small delay to be polite to servers

    print(f"\n🎯 Finished run. Uploaded {uploaded_count} leads to Brevo.")


from flask import Flask

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




