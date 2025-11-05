import os
import requests
import json
from flask import Flask, render_template_string, jsonify, request
from datetime import datetime
import time
import re
from urllib.parse import urljoin

# --------------------------------------------------------------------
# Environment Variables
# --------------------------------------------------------------------
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
BREVO_API_KEY = os.getenv("BREVO_API_KEY")

if not GOOGLE_API_KEY or not BREVO_API_KEY:
    raise ValueError("Missing environment variables for GOOGLE_API_KEY or BREVO_API_KEY")

# --------------------------------------------------------------------
# Flask App + Log Storage
# --------------------------------------------------------------------
app = Flask(__name__)
scraper_logs = []
seen_emails = set()

def log_message(message):
    timestamp = datetime.now().strftime("%H:%M:%S")
    entry = f"[{timestamp}] {message}"
    print(entry)
    scraper_logs.append(entry)
    if len(scraper_logs) > 400:
        scraper_logs.pop(0)

# --------------------------------------------------------------------
# Helper: Extract Owner Names & Phone Numbers
# --------------------------------------------------------------------
def find_owner_name_and_phone(website):
    if not website:
        return "", ""
    try:
        resp = requests.get(website, timeout=6)
        html = resp.text
        text = re.sub(r"<[^>]*>", " ", html)
        text = re.sub(r"\s+", " ", text)

        about_link = None
        for link in re.findall(r'href=["\'](.*?)["\']', html):
            if "about" in link.lower():
                about_link = urljoin(website, link)
                break
        if about_link:
            try:
                about_resp = requests.get(about_link, timeout=6)
                text += " " + re.sub(r"<[^>]*>", " ", about_resp.text)
            except:
                pass

        owner_keywords = ["owner", "founder", "ceo", "manager", "director", "president"]
        owner_name = ""
        for line in text.split("."):
            if any(k in line.lower() for k in owner_keywords):
                name_match = re.search(r"\b([A-Z][a-z]+ [A-Z][a-z]+)\b", line)
                if name_match:
                    owner_name = name_match.group(1)
                    break

        phone_match = re.search(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", text)
        phone = phone_match.group(0) if phone_match else ""
        return owner_name, phone
    except Exception as e:
        log_message(f"Error parsing {website}: {e}")
        return "", ""

# --------------------------------------------------------------------
# Core: Get Businesses from Google
# --------------------------------------------------------------------
def get_businesses_from_google(location="Richmond,VA", radius_meters=5000, limit=400):
    log_message(f"Searching businesses near {location}...")
    url = (
        f"https://maps.googleapis.com/maps/api/place/textsearch/json?"
        f"query=businesses+in+{location}&radius={radius_meters}&key={GOOGLE_API_KEY}"
    )
    resp = requests.get(url)
    data = resp.json()
    if "results" not in data:
        log_message("No results from Google Places.")
        return []

    businesses = []
    for result in data["results"][:limit]:
        if len(businesses) >= limit:
            break
        name = result.get("name", "")
        place_id = result.get("place_id")
        details_url = (
            f"https://maps.googleapis.com/maps/api/place/details/json?"
            f"place_id={place_id}&fields=name,website,formatted_phone_number&key={GOOGLE_API_KEY}"
        )
        det = requests.get(details_url).json().get("result", {})
        businesses.append({
            "name": name,
            "website": det.get("website", ""),
            "phone": det.get("formatted_phone_number", ""),
        })
    log_message(f"Found {len(businesses)} businesses from Google.")
    return businesses

# --------------------------------------------------------------------
# Helper: Find Email
# --------------------------------------------------------------------
def find_email_on_website(website):
    if not website:
        return ""
    try:
        resp = requests.get(website, timeout=6)
        emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", resp.text)
        if emails:
            for e in emails:
                if not any(bad in e for bad in ["example.com", "wixpress", "sentry", "schema.org"]):
                    return e
    except Exception as e:
        log_message(f"Error scanning {website}: {e}")
    return ""

# --------------------------------------------------------------------
# Helper: Add to Brevo
# --------------------------------------------------------------------
def add_to_brevo(contact, list_id=3):
    if not contact.get("email") and list_id == 3:
        return
    url = "https://api.brevo.com/v3/contacts"
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "api-key": BREVO_API_KEY
    }
    payload = {
        "email": contact.get("email", f"{contact['name'].replace(' ', '_').lower()}@noemail.local"),
        "attributes": {
            "FIRSTNAME": contact.get("owner_name") or contact.get("name", ""),
            "COMPANY": contact.get("name", ""),
            "PHONE": contact.get("phone", ""),
            "WEBSITE": contact.get("website", "")
        },
        "listIds": [list_id]
    }
    r = requests.post(url, headers=headers, data=json.dumps(payload))
    log_message(f"Added {contact['name']} to Brevo (List {list_id}) — {r.status_code}")

# --------------------------------------------------------------------
# Scraper Process
# --------------------------------------------------------------------
def run_scraper_process():
    scraper_logs.clear()
    seen_emails.clear()
    log_message("🚀 Starting lead scraper...")
    businesses = get_businesses_from_google()
    uploaded = 0

    for idx, biz in enumerate(businesses, start=1):
        log_message(f"Fetching business {idx} of {len(businesses)}…")
        email = find_email_on_website(biz.get("website"))
        owner_name, phone = find_owner_name_and_phone(biz.get("website"))
        if not phone:
            phone = biz.get("phone", "")

        if email and email not in seen_emails:
            contact = {
                "name": biz.get("name"),
                "phone": phone,
                "website": biz.get("website"),
                "email": email,
                "owner_name": owner_name or biz.get("name", "")
            }
            add_to_brevo(contact, list_id=3)
            seen_emails.add(email)
            uploaded += 1
            log_message(f"✅ {biz['name']} ({email}) added with owner: {owner_name or biz['name']}")
        elif not email:
            contact = {
                "name": biz.get("name"),
                "phone": phone,
                "website": biz.get("website"),
                "email": "",
                "owner_name": owner_name or biz.get("name", "")
            }
            add_to_brevo(contact, list_id=5)
            log_message(f"📞 {biz['name']} added to List 5 (no email found)")
        else:
            log_message(f"⚠️ Duplicate skipped: {email}")

        time.sleep(0.5)
        
    log_message(f"✅ Run complete — {len(businesses)} total businesses processed.")
    log_message(f"🎯 Scraper finished — {uploaded} unique contacts with emails uploaded to List 3.")

# --------------------------------------------------------------------
# Routes + Interface
# --------------------------------------------------------------------
@app.route("/")
def index():
    return render_template_string("""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Business Lead Scraper</title>
<style>
  body {
    background-color: #000;
    color: #00aaff;
    font-family: 'Consolas', monospace;
    text-align: center;
    padding: 30px;
  }
  h1 { font-size: 2.4em; color: #00bfff; margin-bottom: 10px; }
  button {
    background-color: #00bfff;
    border: none;
    padding: 14px 28px;
    font-size: 16px;
    font-weight: bold;
    color: #000;
    cursor: pointer;
    border-radius: 6px;
    box-shadow: 0 0 10px #00bfff;
  }
  button:hover { background-color: #0088cc; }
  #log-box {
    margin-top: 30px;
    width: 90%;
    max-width: 800px;
    margin-left: auto;
    margin-right: auto;
    background: #0a0a0a;
    border: 1px solid #00bfff;
    padding: 20px;
    text-align: left;
    height: 400px;
    overflow-y: auto;
    border-radius: 10px;
  }
</style>
</head>
<body>
  <h1>Business Lead Scraper</h1>
  <h3>Up to 400 leads with live progress logging</h3>
  <button onclick="startScraper()">Start Scraper</button>
  <div id="log-box"></div>

<script>
async function startScraper() {
  document.getElementById('log-box').innerHTML = "<div>🚀 Scraper starting...</div>";
  fetch('/run');
}
async function fetchLogs() {
  const res = await fetch('/logs');
  const data = await res.json();
  const logBox = document.getElementById('log-box');
  logBox.innerHTML = data.logs.map(l => "<div>" + l + "</div>").join('');
  logBox.scrollTop = logBox.scrollHeight;
}
setInterval(fetchLogs, 2000);
</script>
</body>
</html>
""")

@app.route("/run")
def run_scraper():
    import threading
    t = threading.Thread(target=run_scraper_process)
    t.start()
    return jsonify({"status": "Scraper started"})

@app.route("/logs")
def get_logs():
    return jsonify({"logs": scraper_logs})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
