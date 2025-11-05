import os
import requests
import json
from flask import Flask, render_template_string, request, jsonify, redirect, url_for
from datetime import datetime
import time
import re
import pandas as pd
from urllib.parse import urljoin

# ----------------------------------------------------------
# Environment Variables
# ----------------------------------------------------------
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
BREVO_API_KEY = os.getenv("BREVO_API_KEY")

if not GOOGLE_API_KEY or not BREVO_API_KEY:
    raise ValueError("Missing GOOGLE_API_KEY or BREVO_API_KEY")

# ----------------------------------------------------------
# Flask Setup
# ----------------------------------------------------------
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

# ----------------------------------------------------------
# Google Places API Search
# ----------------------------------------------------------
def get_businesses_from_google(category, zipcode, radius_miles):
    radius_meters = int(radius_miles) * 1609
    location_query = f"{category} near {zipcode}"
    url = (
        f"https://maps.googleapis.com/maps/api/place/textsearch/json?"
        f"query={location_query}&radius={radius_meters}&key={GOOGLE_API_KEY}"
    )

    log_message(f"🔎 Searching {category} businesses near {zipcode} within {radius_miles} miles...")
    resp = requests.get(url)
    data = resp.json()
    results = data.get("results", [])
    log_message(f"📍 Found {len(results)} businesses in {category} search.")

    businesses = []
    for result in results:
        name = result.get("name", "Unknown Business")
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
        time.sleep(0.2)

    return businesses

# ----------------------------------------------------------
# Email, Name, and Phone Extraction
# ----------------------------------------------------------
def find_email_on_website(website):
    if not website:
        return ""
    try:
        r = requests.get(website, timeout=6)
        emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", r.text)
        for e in emails:
            if not any(bad in e for bad in ["example", "wixpress", "sentry", "schema"]):
                return e
    except:
        pass
    return ""

def find_owner_name_and_phone(website):
    if not website:
        return "", ""
    try:
        r = requests.get(website, timeout=6)
        text = re.sub(r"<[^>]*>", " ", r.text)
        text = re.sub(r"\s+", " ", text)
        owner_keywords = ["owner", "ceo", "founder", "manager", "director", "president"]
        for line in text.split("."):
            if any(k in line.lower() for k in owner_keywords):
                name_match = re.search(r"\b([A-Z][a-z]+ [A-Z][a-z]+)\b", line)
                if name_match:
                    phone = re.search(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", text)
                    return name_match.group(1), phone.group(0) if phone else ""
        phone = re.search(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", text)
        return "", phone.group(0) if phone else ""
    except:
        return "", ""

# ----------------------------------------------------------
# Add to Brevo
# ----------------------------------------------------------
def add_to_brevo(contact, has_email=True):
    url = "https://api.brevo.com/v3/contacts"
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "api-key": BREVO_API_KEY
    }
    payload = {
        "email": contact.get("email", "") if has_email else f"{contact['name'].replace(' ', '').lower()}@placeholder.com",
        "attributes": {
            "FIRSTNAME": contact.get("owner_name") or contact.get("name"),
            "COMPANY": contact.get("name"),
            "PHONE": contact.get("phone", ""),
            "WEBSITE": contact.get("website", "")
        },
        "listIds": [3 if has_email else 5]
    }
    r = requests.post(url, headers=headers, data=json.dumps(payload))
    log_message(f"Added to Brevo (List {'3' if has_email else '5'}): {contact.get('email', 'no email')} ({r.status_code})")

# ----------------------------------------------------------
# Run Scraper
# ----------------------------------------------------------
def run_scraper_process(categories, zipcode, radius):
    scraper_logs.clear()
    seen_emails.clear()
    log_message("🚀 Scraper started.")
    all_results = []
    uploaded = 0

    for category in categories:
        businesses = get_businesses_from_google(category, zipcode, radius)
        all_results.extend(businesses)

    for biz in all_results[:400]:
        email = find_email_on_website(biz.get("website"))
        owner_name, phone = find_owner_name_and_phone(biz.get("website"))
        contact = {
            "name": biz.get("name", "Unknown Business"),
            "phone": phone or biz.get("phone", ""),
            "website": biz.get("website", ""),
            "email": email,
            "owner_name": owner_name
        }

        if email and email not in seen_emails:
            add_to_brevo(contact, has_email=True)
            seen_emails.add(email)
            uploaded += 1
            log_message(f"✅ {biz['name']} ({email}) uploaded to List 3.")
        elif not email:
            add_to_brevo(contact, has_email=False)
            uploaded += 1
            log_message(f"📇 {biz['name']} (No Email) uploaded to List 5.")
        else:
            log_message(f"⚠️ Duplicate skipped: {email}")
        time.sleep(0.5)

    # Save to XLS
    df = pd.DataFrame(all_results)
    filename = f"runs/run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    os.makedirs("runs", exist_ok=True)
    df.to_excel(filename, index=False)
    log_message(f"📁 Run saved as {filename}.")
    log_message(f"✅ Run complete — {len(all_results)} total businesses processed.")
    log_message(f"🎯 Scraper finished — {uploaded} contacts uploaded.")

# ----------------------------------------------------------
# HTML Templates
# ----------------------------------------------------------
BASE_STYLE = """
<style>
body { background-color: #000; color: #00bfff; font-family: 'Consolas', monospace; text-align: center; padding: 20px; }
h1 { color: #00bfff; }
h2 { color: #0099ff; }
button, input[type='text'], select { padding: 10px; margin: 5px; border-radius: 6px; font-weight: bold; }
.navbar { margin-bottom: 20px; }
.navbar a { color: #00bfff; margin: 0 10px; text-decoration: none; }
#log-box { width: 80%; margin: 20px auto; text-align: left; height: 400px; overflow-y: auto; background: #0a0a0a; border: 1px solid #00bfff; border-radius: 10px; padding: 20px; }
.grid { display: flex; flex-wrap: wrap; justify-content: center; gap: 10px; }
.category { background: #00bfff; color: #000; padding: 10px 20px; border-radius: 6px; cursor: pointer; font-weight: bold; }
.category:hover { background: #0088cc; }
</style>
"""

@app.route("/")
def home():
    categories = [
        "Restaurants", "Bars & Clubs", "Retail", "Salons & Spas", "Auto Services",
        "Home Services", "Construction", "Landscaping", "Fitness", "Insurance",
        "Event Venues", "Entertainment", "Healthcare", "Pet Services", "Education"
    ]
    html = f"""
    {BASE_STYLE}
    <div class='navbar'>
        <a href='/'>Home</a> |
        <a href='/previous'>Previous Runs</a> |
        <a href='/about'>About</a> |
        <a href='/help'>Help</a>
    </div>
    <h1>Business Lead Scraper</h1>
    <h2>Select categories and enter ZIP & radius</h2>
    <form action='/run' method='get'>
        <div class='grid'>
            {''.join([f"<label><input type='checkbox' name='categories' value='{c}'> {c}</label>" for c in categories])}
        </div><br>
        ZIP Code: <input type='text' name='zipcode' required>
        Radius (miles): <input type='text' name='radius' required value='10'><br><br>
        <button type='submit'>Start Search</button>
    </form>
    """
    return render_template_string(html)

@app.route("/run")
def run_scraper():
    categories = request.args.getlist("categories")
    zipcode = request.args.get("zipcode", "23220")
    radius = request.args.get("radius", "10")
    import threading
    t = threading.Thread(target=run_scraper_process, args=(categories, zipcode, radius))
    t.start()
    html = f"""
    {BASE_STYLE}
    <div class='navbar'>
        <a href='/'>Back to Search</a> |
        <a href='/previous'>Previous Runs</a> |
        <a href='/about'>About</a> |
        <a href='/help'>Help</a>
    </div>
    <h1>Business Lead Scraper</h1>
    <h2>Scraper is running... Logs below</h2>
    <div id='log-box'></div>
    <script>
    async function fetchLogs() {{
        const res = await fetch('/logs');
        const data = await res.json();
        const box = document.getElementById('log-box');
        box.innerHTML = data.logs.map(l => `<div>${{l}}</div>`).join('');
        box.scrollTop = box.scrollHeight;
    }}
    setInterval(fetchLogs, 2000);
    </script>
    """
    return render_template_string(html)

@app.route("/previous")
def previous():
    files = os.listdir("runs") if os.path.exists("runs") else []
    links = "".join([f"<li><a href='/download/{f}'>{f}</a></li>" for f in files])
    return render_template_string(f"{BASE_STYLE}<div class='navbar'><a href='/'>Home</a></div><h1>Previous Runs</h1><ul>{links}</ul>")

@app.route("/download/<filename>")
def download(filename):
    return redirect(f"/runs/{filename}")

@app.route("/about")
def about():
    return render_template_string(f"{BASE_STYLE}<div class='navbar'><a href='/'>Home</a></div><h1>About</h1><p>This scraper helps find local businesses, extract contact info, and upload it into Brevo lists.</p>")

@app.route("/help")
def help_page():
    return render_template_string(f"{BASE_STYLE}<div class='navbar'><a href='/'>Home</a></div><h1>Help</h1><p>1. Select your categories.<br>2. Enter ZIP and radius.<br>3. Hit Start Search to begin scraping.<br>4. Logs show progress live.</p>")

@app.route("/logs")
def get_logs():
    return jsonify({"logs": scraper_logs})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)

