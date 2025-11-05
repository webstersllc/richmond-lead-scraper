import os
import requests
import json
from flask import Flask, render_template_string, jsonify, send_file, request
from datetime import datetime
import time
import re
from urllib.parse import urljoin
import pandas as pd
from pathlib import Path
import threading

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
last_user_export = {"ready": False, "path": "", "name": ""}

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
# Google API Search
# --------------------------------------------------------------------
def get_businesses_from_google(location="Richmond,VA", radius_meters=8000, limit=60):
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
            "phone": det.get("formatted_phone_number", "")
        })
    log_message(f"Found {len(businesses)} businesses from Google.")
    return businesses

# --------------------------------------------------------------------
# Find Email
# --------------------------------------------------------------------
def find_email_on_website(website):
    if not website:
        return ""
    try:
        resp = requests.get(website, timeout=6)
        emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", resp.text)
        for e in emails:
            if not any(bad in e for bad in ["example.com", "wixpress", "schema.org", "sentry"]):
                return e
    except Exception as e:
        log_message(f"Error scanning {website}: {e}")
    return ""

# --------------------------------------------------------------------
# Add to Brevo
# --------------------------------------------------------------------
def add_to_brevo(contact, list_id=3):
    if not contact.get("email"):
        return
    url = "https://api.brevo.com/v3/contacts"
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "api-key": BREVO_API_KEY
    }
    payload = {
        "email": contact["email"],
        "attributes": {
            "FIRSTNAME": contact.get("owner_name", ""),
            "COMPANY": contact.get("name", ""),
            "PHONE": contact.get("phone", ""),
            "WEBSITE": contact.get("website", "")
        },
        "listIds": [list_id]
    }
    r = requests.post(url, headers=headers, data=json.dumps(payload))
    log_message(f"Added {contact['email']} to Brevo ({r.status_code})")

# --------------------------------------------------------------------
# Scraper Process
# --------------------------------------------------------------------
def run_scraper_process(user_mode=False, location="Richmond,VA"):
    scraper_logs.clear()
    seen_emails.clear()
    log_message("🚀 Starting lead scraper...")
    businesses = get_businesses_from_google(location=location)
    uploaded = 0
    collected_rows = []

    for biz in businesses:
        email = find_email_on_website(biz.get("website"))
        owner_name, phone = find_owner_name_and_phone(biz.get("website"))
        if not owner_name:
            owner_name = biz.get("name", "")
        if not phone:
            phone = biz.get("phone", "")

        contact = {
            "name": biz.get("name"),
            "phone": phone,
            "website": biz.get("website"),
            "email": email,
            "owner_name": owner_name
        }

        if email and email not in seen_emails:
            add_to_brevo(contact, list_id=3)
            seen_emails.add(email)
            uploaded += 1
            log_message(f"✅ {biz['name']} ({email}) added with owner: {owner_name}")
        elif not email and phone:
            add_to_brevo(contact, list_id=5)
            log_message(f"📞 {biz['name']} added to phone-only list (no email).")
        elif email in seen_emails:
            log_message(f"⚠️ Duplicate skipped: {email}")
        else:
            log_message(f"❌ No contact found for {biz['name']}.")
        time.sleep(1.5)

    # export to Excel
    Path("data/exports").mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    fname = f"leads_{location}_{ts}.xlsx"
    fpath = f"data/exports/{fname}"
    df = pd.DataFrame(businesses)
    df.to_excel(fpath, index=False)
    last_user_export.update({"ready": True, "path": fpath, "name": fname})
    log_message(f"🎯 Scraper finished — {uploaded} contacts uploaded to Brevo.")

# --------------------------------------------------------------------
# HTML Template
# --------------------------------------------------------------------
TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Business Lead Scraper</title>
<style>
  body { background-color: #000; color: #00aaff; font-family: 'Consolas', monospace; text-align: center; padding: 30px; }
  h1 { font-size: 2.4em; color: #00bfff; margin-bottom: 10px; }
  nav a { color: #00bfff; margin: 0 10px; text-decoration: none; }
  button { background-color: #00bfff; border: none; padding: 14px 28px; font-size: 16px; font-weight: bold; color: #000; cursor: pointer; border-radius: 6px; box-shadow: 0 0 10px #00bfff; margin: 10px; }
  #log-box { margin-top: 30px; width: 90%; max-width: 800px; margin-left: auto; margin-right: auto; background: #0a0a0a; border: 1px solid #00bfff; padding: 20px; text-align: left; height: 400px; overflow-y: auto; border-radius: 10px; }
  .log-entry { margin: 4px 0; }
</style>
</head>
<body>
  <nav>
    <a href="/">Home</a> |
    <a href="/about">About</a> |
    <a href="/help">Help</a> |
    <a href="/previous">Previous Runs</a>
  </nav>
  <h1>Business Lead Scraper</h1>
  <h2>Find businesses, owners, phones, and emails — auto-upload to Brevo</h2>
  <button onclick="startScraper()">Start Scraper</button>
  <div id="log-box"></div>

<script>
async function startScraper() {
  document.getElementById('log-box').innerHTML = "<div class='log-entry'>🚀 Scraper starting...</div>";
  fetch('/run');
}
async function fetchLogs() {
  const res = await fetch('/logs');
  const data = await res.json();
  const logBox = document.getElementById('log-box');
  logBox.innerHTML = data.logs.map(l => "<div class='log-entry'>" + l + "</div>").join('');
  logBox.scrollTop = logBox.scrollHeight;
}
setInterval(fetchLogs, 2000);
</script>
</body>
</html>
"""

# --------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------
@app.route("/")
def index():
    return render_template_string(TEMPLATE)

@app.route("/about")
def about():
    return render_template_string("<h1 style='color:#00bfff;'>About</h1><p>This scraper helps find local business leads and syncs them to Brevo.</p>")

@app.route("/help")
def help_page():
    return render_template_string("<h1 style='color:#00bfff;'>Help</h1><p>Press 'Start Scraper' to begin. Logs will appear in real-time. Exports are auto-generated as XLSX files and uploaded to Brevo lists.</p>")

@app.route("/previous")
def previous_runs():
    files = Path("data/exports").glob("*.xlsx")
    links = "".join([f"<p><a style='color:#00bfff;' href='/download/{f.name}'>{f.name}</a></p>" for f in files])
    return render_template_string(f"<h1 style='color:#00bfff;'>Previous Exports</h1>{links or '<p>No exports yet.</p>'}")

@app.route("/download/<name>")
def download_file(name):
    fpath = f"data/exports/{name}"
    if not os.path.exists(fpath):
        return jsonify({"error": "Not found"}), 404
    return send_file(fpath, as_attachment=True)

@app.route("/run")
def run_scraper():
    threading.Thread(target=run_scraper_process).start()
    return jsonify({"status": "Scraper started"})

@app.route("/logs")
def get_logs():
    return jsonify({"logs": scraper_logs})

# --------------------------------------------------------------------
# Run
# --------------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)


