import os
import requests
import json
import threading
import time
from datetime import datetime
from flask import Flask, request, render_template_string, jsonify
import pandas as pd

# --------------------------------------------------------------------
# API Keys
# --------------------------------------------------------------------
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
BREVO_API_KEY = os.getenv("BREVO_API_KEY")

if not GOOGLE_API_KEY or not BREVO_API_KEY:
    raise ValueError("Missing environment variables for GOOGLE_API_KEY or BREVO_API_KEY")

# --------------------------------------------------------------------
# App + Globals
# --------------------------------------------------------------------
app = Flask(__name__)
scraper_logs = []
seen_emails = set()
lock = threading.Lock()

# --------------------------------------------------------------------
# Logging helper
# --------------------------------------------------------------------
def log(msg):
    with lock:
        ts = datetime.now().strftime("%H:%M:%S")
        entry = f"[{ts}] {msg}"
        print(entry)
        scraper_logs.append(entry)
        if len(scraper_logs) > 400:
            scraper_logs.pop(0)

# --------------------------------------------------------------------
# Google Search Helper
# --------------------------------------------------------------------
def get_businesses_from_google(business_type, zipcode, radius_miles):
    radius_meters = int(float(radius_miles) * 1609.34)
    url = f"https://maps.googleapis.com/maps/api/place/textsearch/json?query={business_type}+near+{zipcode}&radius={radius_meters}&key={GOOGLE_API_KEY}"

    results = []
    next_page = None
    count = 0
    start_time = time.time()

    while True:
        if next_page:
            url = f"https://maps.googleapis.com/maps/api/place/textsearch/json?pagetoken={next_page}&key={GOOGLE_API_KEY}"
        resp = requests.get(url)
        data = resp.json()

        for res in data.get("results", []):
            name = res.get("name")
            place_id = res.get("place_id")
            details_url = (
                f"https://maps.googleapis.com/maps/api/place/details/json?"
                f"place_id={place_id}&fields=name,website,formatted_phone_number&key={GOOGLE_API_KEY}"
            )
            det = requests.get(details_url).json().get("result", {})
            results.append({
                "name": name,
                "website": det.get("website", ""),
                "phone": det.get("formatted_phone_number", "")
            })
            count += 1

        log(f"📍 Retrieved {count} {business_type} results total.")

        next_page = data.get("next_page_token")
        if not next_page:
            break
        if time.time() - start_time > 180:
            log("⏱ Timeout reached while fetching businesses from Google.")
            break
        time.sleep(2)

    return results

# --------------------------------------------------------------------
# Email Finder
# --------------------------------------------------------------------
def find_email_on_website(website):
    if not website:
        return ""
    try:
        resp = requests.get(website, timeout=6)
        emails = set(json.loads(json.dumps(
            list(set(
                __import__('re').findall(
                    r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
                    resp.text
                )
            ))
        )))
        for e in emails:
            if not any(bad in e for bad in ["example.com", "wixpress", "sentry", "schema.org", "wordpress"]):
                return e
    except Exception as e:
        log(f"Error scanning {website}: {e}")
    return ""

# --------------------------------------------------------------------
# Add to Brevo
# --------------------------------------------------------------------
def add_to_brevo(contact, list_id):
    if not contact.get("name"):
        contact["name"] = "Unknown"
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "api-key": BREVO_API_KEY
    }
    payload = {
        "email": contact.get("email", ""),
        "attributes": {
            "FIRSTNAME": contact["name"],
            "PHONE": contact.get("phone", ""),
            "WEBSITE": contact.get("website", "")
        },
        "listIds": [list_id]
    }
    r = requests.post("https://api.brevo.com/v3/contacts", headers=headers, data=json.dumps(payload))
    log(f"Added to Brevo (List {list_id}): {contact.get('email', '(no email)')} ({r.status_code})")

# --------------------------------------------------------------------
# Scraper core with timeout + min leads
# --------------------------------------------------------------------
def run_scraper(business_types, zipcode, radius):
    scraper_logs.clear()
    seen_emails.clear()
    log("🚀 Scraper started.")

    start = time.time()
    timeout = 180
    uploaded = 0
    all_contacts = []

    while True:
        elapsed = time.time() - start
        if elapsed >= timeout:
            log("⏱ Timeout reached — stopping scraper.")
            break
        if uploaded >= 30:
            log("✅ Minimum 30 leads reached — stopping early.")
            break

        for biz_type in business_types:
            log(f"🔎 Searching {biz_type} near {zipcode} ({radius} mi radius)...")
            businesses = get_businesses_from_google(biz_type, zipcode, radius)

            for biz in businesses:
                email = find_email_on_website(biz.get("website"))
                if email and email not in seen_emails:
                    contact = {"name": biz.get("name"), "email": email, "phone": biz.get("phone"), "website": biz.get("website")}
                    add_to_brevo(contact, 3)
                    seen_emails.add(email)
                    uploaded += 1
                    log(f"✅ {biz['name']} ({email}) → List 3")
                elif not email and biz.get("phone"):
                    contact = {"name": biz.get("name"), "email": "", "phone": biz.get("phone"), "website": biz.get("website")}
                    add_to_brevo(contact, 5)
                    uploaded += 1
                    log(f"📇 {biz['name']} (No Email) → List 5")

                all_contacts.append(biz)
                if time.time() - start >= timeout:
                    log("⏱ Timeout during loop — finalizing upload.")
                    break
            if time.time() - start >= timeout:
                break

        log(f"⏱ Timeout countdown: {int((timeout - elapsed)/60)} min left.")
        time.sleep(60)

    if all_contacts:
        df = pd.DataFrame(all_contacts)
        df.to_excel("last_run_results.xlsx", index=False)
        log("📁 Saved results to last_run_results.xlsx")

    log(f"🎯 Finished — {uploaded} contacts processed.")

# --------------------------------------------------------------------
# Web Interface
# --------------------------------------------------------------------
@app.route("/")
def home():
    html = """
    <html>
    <head>
      <title>Business Lead Scraper</title>
      <style>
        body { background:black; color:#00aaff; font-family:Consolas; text-align:center; }
        .menu { margin-bottom:20px; }
        .menu a { color:#00bfff; text-decoration:none; margin:0 10px; }
        .menu a:hover { text-decoration:underline; }
        button { background:#00bfff; color:black; padding:10px 20px; border:none; border-radius:8px; cursor:pointer; }
        button:hover { background:#0088cc; }
        .category-block { display:flex; flex-wrap:wrap; justify-content:center; gap:10px; margin-top:15px; }
        label { display:block; margin:5px; }
      </style>
    </head>
    <body>
      <div class="menu">
        <a href="/">Home</a>
        <a href="/previous">Previous Runs</a>
        <a href="/about">About</a>
        <a href="/help">Help</a>
      </div>
      <h1>Business Lead Scraper</h1>
      <h3>Enter search details:</h3>
      <form action="/run" method="get">
        <input type="text" name="zipcode" placeholder="Zip Code" required>
        <input type="number" name="radius" placeholder="Radius (miles)" required>
        <div class="category-block">
          <div><input type="checkbox" name="category" value="Landscaping"> Landscaping</div>
          <div><input type="checkbox" name="category" value="Restaurants"> Restaurants</div>
          <div><input type="checkbox" name="category" value="Coffee Shops"> Coffee Shops</div>
          <div><input type="checkbox" name="category" value="Bars"> Bars</div>
          <div><input type="checkbox" name="category" value="HVAC"> HVAC</div>
          <div><input type="checkbox" name="category" value="Plumbers"> Plumbers</div>
          <div><input type="checkbox" name="category" value="Electricians"> Electricians</div>
          <div><input type="checkbox" name="category" value="Roofers"> Roofers</div>
          <div><input type="checkbox" name="category" value="Contractors"> Contractors</div>
          <div><input type="checkbox" name="category" value="Insurance"> Insurance</div>
          <div><input type="checkbox" name="category" value="Event Venues"> Event Venues</div>
          <div><input type="checkbox" name="category" value="Entertainment"> Entertainment</div>
        </div>
        <br>
        <button type="submit">Run Scraper</button>
      </form>
    </body>
    </html>
    """
    return render_template_string(html)

@app.route("/run")
def run():
    zipcode = request.args.get("zipcode", "23005")
    radius = request.args.get("radius", "30")
    categories = request.args.getlist("category")
    if not categories:
        categories = ["Landscaping"]
    threading.Thread(target=run_scraper, args=(categories, zipcode, radius)).start()
    return render_template_string("""
      <html><head>
      <meta http-equiv="refresh" content="2;url=/logs">
      <style>
        body{background:black;color:#00aaff;font-family:Consolas;text-align:center;padding-top:50px;}
      </style></head>
      <body><h1>🚀 Scraper Starting...</h1><p>Redirecting to logs...</p></body></html>
    """)

@app.route("/logs")
def logs():
    log_html = "<br>".join(scraper_logs)
    return render_template_string(f"""
    <html>
    <head>
      <meta http-equiv="refresh" content="3">
      <style>
        body{{background:black;color:#00aaff;font-family:Consolas;}}
        a{{color:#00bfff;text-decoration:none;}}
        .logbox{{border:1px solid #00bfff;padding:20px;width:90%;margin:auto;height:400px;overflow:auto;border-radius:10px;}}
      </style>
    </head>
    <body>
      <div class="menu"><a href="/">← Back to Search</a></div>
      <h2>Scraper Logs</h2>
      <div class="logbox">{log_html}</div>
    </body></html>
    """)

@app.route("/previous")
def previous():
    return "<h2 style='color:#00aaff;text-align:center;'>Previous Runs (Coming Soon)</h2>"

@app.route("/about")
def about():
    return "<h2 style='color:#00aaff;text-align:center;'>About: Business Lead Scraper helps find and upload leads directly into Brevo.</h2>"

@app.route("/help")
def help_page():
    return "<h2 style='color:#00aaff;text-align:center;'>Help: Enter a ZIP, radius, and choose categories. The scraper runs 3 minutes and uploads to Brevo.</h2>"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)


