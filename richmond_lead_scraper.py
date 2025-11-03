import os
import json
import re
import time
import requests
from flask import Flask, render_template_string
from bs4 import BeautifulSoup

# === CONFIGURATION ===
BREVO_API_KEY = os.getenv("BREVO_API_KEY")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
CITY = "Richmond,VA"
SEARCH_TERMS = [
    f"new small businesses near {CITY}",
    f"recently opened businesses in {CITY}",
    f"local startups in {CITY}",
    f"new companies in {CITY}",
    f"gyms in {CITY}",
    f"coffee shops in {CITY}",
    f"marketing agencies in {CITY}",
    f"real estate companies in {CITY}"
]

EMAIL_PATTERN = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
HEADERS = {"User-Agent": "Mozilla/5.0"}

# === STORAGE ===
LOG_FILE = "scraper_log.txt"
UPLOADED_FILE = "uploaded_leads.json"

# Load existing leads
if os.path.exists(UPLOADED_FILE):
    with open(UPLOADED_FILE, "r") as f:
        uploaded_leads = set(json.load(f))
else:
    uploaded_leads = set()

def log(message):
    timestamp = time.strftime("[%H:%M:%S]")
    line = f"{timestamp} {message}"
    print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

# === GOOGLE SEARCH ===
def google_search(query):
    url = f"https://www.google.com/search?q={query}"
    log(f"Searching: {query}")
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("http") and "google" not in href:
                links.append(href)
        return list(set(links))[:15]
    except Exception as e:
        log(f"⚠️ Google search failed for {query}: {e}")
        return []

# === SCRAPE BUSINESS INFO ===
def scrape_site(url):
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        text = soup.get_text(" ", strip=True)
        emails = list(set(re.findall(EMAIL_PATTERN, text)))
        owner = ""
        for line in text.splitlines():
            if any(k in line.lower() for k in ["owner", "manager", "founder", "ceo", "director"]):
                owner = line.strip()[:60]
                break
        name = soup.title.string.strip() if soup.title else "Unknown"
        return {"name": name, "email": emails, "owner": owner, "url": url}
    except Exception as e:
        log(f"⚠️ Error scraping {url}: {e}")
        return None

# === ADD TO BREVO ===
def add_to_brevo(lead):
    if not lead["email"]:
        log(f"❌ No email found for {lead['name']}.")
        return
    email = lead["email"][0]
    if email in uploaded_leads:
        log(f"⚠️ Duplicate email skipped: {email}")
        return

    url = "https://api.brevo.com/v3/contacts"
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "api-key": BREVO_API_KEY
    }
    payload = {
        "email": email,
        "attributes": {
            "FIRSTNAME": lead.get("owner", ""),
            "COMPANY": lead.get("name", ""),
            "WEBSITE": lead.get("url", "")
        },
        "listIds": [3]  # change this to your list ID
    }
    r = requests.post(url, headers=headers, data=json.dumps(payload))
    log(f"Added {email} to Brevo ({r.status_code})")
    log(f"Response: {r.text}")

    if r.status_code == 201 or r.status_code == 204:
        uploaded_leads.add(email)
        with open(UPLOADED_FILE, "w") as f:
            json.dump(list(uploaded_leads), f)
        log(f"✅ {lead['name']} ({email}) added with owner: {lead['owner']}")
    else:
        log(f"❌ Failed to add {email}. See response above.")

# === MAIN SCRAPER ===
def run_scraper():
    log("🚀 Starting lead scraper...")
    all_sites = []
    for term in SEARCH_TERMS:
        all_sites += google_search(term)
    all_sites = list(set(all_sites))
    log(f"Found {len(all_sites)} businesses from Google.")

    for site in all_sites:
        lead = scrape_site(site)
        if lead:
            add_to_brevo(lead)
        time.sleep(1)

    log(f"🎯 Scraper finished — {len(uploaded_leads)} unique contacts uploaded to Brevo.")

# === WEB INTERFACE ===
app = Flask(__name__)

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Richmond Lead Scraper</title>
<style>
body { background-color: #000; color: #0ff; font-family: monospace; padding: 20px; }
#logbox { border: 1px solid #0ff; padding: 10px; height: 75vh; overflow-y: scroll; background-color: #001; }
button { background-color: #0ff; color: #000; border: none; padding: 10px 20px; margin: 10px 0; cursor: pointer; }
button:hover { background-color: #09f; }
</style>
<meta http-equiv="refresh" content="5">
</head>
<body>
<h1>Richmond Lead Scraper</h1>
<button onclick="window.location.href='/run'">Start Scraper</button>
<pre id="logbox">{{log_content}}</pre>
</body>
</html>
"""

@app.route("/")
def home():
    log_content = ""
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, "r") as f:
            log_content = f.read()
    return render_template_string(HTML_TEMPLATE, log_content=log_content)

@app.route("/run")
def run():
    log("🟢 Manual start triggered.")
    run_scraper()
    return "✅ Scraper completed successfully. <a href='/'>Back</a>"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)

