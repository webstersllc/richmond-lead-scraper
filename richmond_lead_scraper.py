import os
import re
import json
import time
import requests
from flask import Flask, render_template_string
from bs4 import BeautifulSoup

# === CONFIG ===
BREVO_API_KEY = os.getenv("BREVO_API_KEY")
CITY = "Richmond,VA"
SEARCH_TERMS = [
    f"new small businesses near {CITY}",
    f"recently opened businesses in {CITY}",
    f"local startups in {CITY}",
    f"service companies {CITY}",
    f"marketing agencies {CITY}",
    f"coffee shops {CITY}",
    f"gyms {CITY}",
    f"contractors {CITY}",
    f"real estate offices {CITY}",
    f"restaurants {CITY}"
]
HEADERS = {"User-Agent": "Mozilla/5.0"}
EMAIL_PATTERN = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")

LOG_FILE = "scraper_log.txt"
UPLOADED_FILE = "uploaded_leads.json"

if os.path.exists(UPLOADED_FILE):
    with open(UPLOADED_FILE, "r") as f:
        uploaded_leads = set(json.load(f))
else:
    uploaded_leads = set()

# === LOGGING ===
def log(message):
    timestamp = time.strftime("[%H:%M:%S]")
    line = f"{timestamp} {message}"
    print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

# === SEARCH ===
def google_search(query):
    log(f"🔎 Searching: {query}")
    url = f"https://www.google.com/search?q={query}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
        links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("http") and "google" not in href:
                links.append(href)
        return list(set(links))[:25]
    except Exception as e:
        log(f"⚠️ Search error: {e}")
        return []

# === SCRAPE ===
def scrape_site(url):
    try:
        urls_to_check = [url]
        for suffix in ["/contact", "/about", "/team"]:
            if suffix not in url:
                urls_to_check.append(url.rstrip("/") + suffix)

        for u in urls_to_check:
            r = requests.get(u, headers=HEADERS, timeout=8)
            text = BeautifulSoup(r.text, "html.parser").get_text(" ", strip=True)
            emails = list(set(re.findall(EMAIL_PATTERN, text)))
            if emails:
                owner = ""
                for line in text.splitlines():
                    if any(k in line.lower() for k in ["owner","manager","founder","ceo","director"]):
                        owner = line.strip()[:60]
                        break
                title = BeautifulSoup(r.text, "html.parser").title
                name = title.string.strip() if title else "Unknown Business"
                return {"name": name, "email": emails, "owner": owner, "url": u}
        return None
    except Exception as e:
        log(f"⚠️ Error scraping {url}: {e}")
        return None

# === BREVO UPLOAD ===
def add_to_brevo(lead):
    email = lead["email"][0]
    if email in uploaded_leads:
        log(f"⚠️ Duplicate skipped: {email}")
        return
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "api-key": BREVO_API_KEY
    }
    data = {
        "email": email,
        "attributes": {
            "FIRSTNAME": lead.get("owner", ""),
            "COMPANY": lead.get("name", ""),
            "WEBSITE": lead.get("url", "")
        },
        "listIds": [3]
    }
    r = requests.post("https://api.brevo.com/v3/contacts", headers=headers, data=json.dumps(data))
    log(f"Added {email} to Brevo ({r.status_code})")
    if r.status_code in [200, 201, 204]:
        uploaded_leads.add(email)
        with open(UPLOADED_FILE, "w") as f:
            json.dump(list(uploaded_leads), f)
        log(f"✅ {lead['name']} ({email}) added with owner: {lead['owner']}")
    else:
        log(f"❌ Brevo error: {r.text}")

# === MAIN SCRAPER ===
def run_scraper():
    log("🚀 Starting Richmond Lead Scraper...")
    all_sites = []
    for term in SEARCH_TERMS:
        all_sites += google_search(term)
    all_sites = list(set(all_sites))
    log(f"🌐 Found {len(all_sites)} candidate sites.")

    uploaded_this_run = 0
    for site in all_sites:
        if uploaded_this_run >= 20:
            break
        lead = scrape_site(site)
        if lead and lead["email"]:
            add_to_brevo(lead)
            uploaded_this_run += 1
        else:
            log(f"❌ No email found for {site}")
        time.sleep(1)

    log(f"🎯 Finished — {uploaded_this_run} new contacts this run, {len(uploaded_leads)} total unique uploaded.")

# === WEB UI ===
app = Flask(__name__)

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Richmond Lead Scraper</title>
<style>
body { background-color:#000; color:#00ffff; font-family:monospace; padding:20px; }
#logbox { border:1px solid #00ffff; padding:10px; height:75vh; overflow-y:scroll; background-color:#001; }
button { background-color:#00ffff; color:#000; border:none; padding:10px 20px; margin:10px 0; cursor:pointer; font-weight:bold; }
button:hover { background-color:#0099ff; }
</style>
<meta http-equiv="refresh" content="5">
</head>
<body>
<h1>Richmond Lead Scraper</h1>
<button onclick="window.location.href='/run'">▶ Start Scraper</button>
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
