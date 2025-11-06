import os
import json
import time
import threading
from datetime import datetime
from flask import Flask, request, render_template_string, jsonify
import requests

# ---------------------------------------------
# ENV
# ---------------------------------------------
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
BREVO_API_KEY = os.getenv("BREVO_API_KEY")
if not GOOGLE_API_KEY or not BREVO_API_KEY:
    raise ValueError("Missing GOOGLE_API_KEY or BREVO_API_KEY")

# ---------------------------------------------
# APP STATE
# ---------------------------------------------
app = Flask(__name__)
scraper_logs = []
seen_emails = set()
lock = threading.Lock()

TIMEOUT_SECONDS = 180       # hard stop at 3 minutes
MIN_LEADS = 30              # minimum to try to reach

# Category groups -> subcategories (search terms)
CATEGORY_GROUPS = {
    "Home Services": [
        "Landscaping", "HVAC", "Plumbers", "Electricians", "Roofers", "Contractors"
    ],
    "Food & Drink": [
        "Restaurants", "Coffee Shops", "Bars"
    ],
    "Entertainment & Venues": [
        "Event Venues", "Escape Rooms", "Mini Golf", "Bowling"
    ],
    "Professional Services": [
        "Insurance Agencies", "Real Estate Offices", "Gyms"
    ]
}

# ---------------------------------------------
# LOGGING
# ---------------------------------------------
def log(msg):
    with lock:
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {msg}"
        print(line)
        scraper_logs.append(line)
        if len(scraper_logs) > 600:
            scraper_logs.pop(0)

# ---------------------------------------------
# GOOGLE HELPERS
# ---------------------------------------------
def places_textsearch(query, radius_meters, page_token=None):
    base = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {
        "query": query,
        "radius": radius_meters,
        "key": GOOGLE_API_KEY
    }
    if page_token:
        params = {"pagetoken": page_token, "key": GOOGLE_API_KEY}
    r = requests.get(base, params=params, timeout=12)
    r.raise_for_status()
    return r.json()

def place_details(place_id):
    base = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "fields": "name,website,formatted_phone_number",
        "key": GOOGLE_API_KEY
    }
    r = requests.get(base, params=params, timeout=12)
    r.raise_for_status()
    return r.json().get("result", {})

def get_businesses_from_google(subterm, zipcode, radius_miles, start_time):
    radius_meters = int(float(radius_miles) * 1609.34)
    query = f"{subterm} near {zipcode}"

    all_results = []
    seen_place_ids = set()
    page_token = None

    while True:
        # Timeout check
        if time.time() - start_time >= TIMEOUT_SECONDS:
            log("⏱ Timeout while fetching Google results.")
            break

        data = places_textsearch(query, radius_meters, page_token)
        results = data.get("results", [])
        for res in results:
            pid = res.get("place_id")
            if not pid or pid in seen_place_ids:
                continue
            seen_place_ids.add(pid)

            # Detail lookup (with timeout safety)
            if time.time() - start_time >= TIMEOUT_SECONDS:
                break
            det = place_details(pid)
            all_results.append({
                "name": det.get("name") or res.get("name") or "Unknown",
                "website": det.get("website", ""),
                "phone": det.get("formatted_phone_number", "")
            })

        log(f"📍 Retrieved {len(all_results)} {subterm} results total.")
        page_token = data.get("next_page_token")
        if not page_token:
            break
        # Google requires a brief wait before next_page_token is valid
        time.sleep(2)

    return all_results

# ---------------------------------------------
# EMAIL SCRAPE
# ---------------------------------------------
BAD_EMAIL_FRAGMENTS = ["example.com", "wixpress", "sentry", "schema.org", "wordpress"]

def find_email_on_website(website):
    if not website:
        return ""
    try:
        r = requests.get(website, timeout=8, headers={"User-Agent":"Mozilla/5.0"})
        r.raise_for_status()
        import re
        emails = set(re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", r.text))
        for e in emails:
            if not any(bad in e.lower() for bad in BAD_EMAIL_FRAGMENTS):
                return e
    except Exception as e:
        log(f"Error scanning {website}: {e}")
    return ""

# ---------------------------------------------
# BREVO
# ---------------------------------------------
def add_to_brevo(contact, list_id):
    # contact: {name, email?, phone?, website?}
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "api-key": BREVO_API_KEY
    }
    payload = {
        "email": contact.get("email", ""),
        "attributes": {
            "FIRSTNAME": contact.get("name") or "Unknown",
            "PHONE": contact.get("phone", ""),
            "WEBSITE": contact.get("website", "")
        },
        "listIds": [list_id]
    }
    resp = requests.post("https://api.brevo.com/v3/contacts", headers=headers, data=json.dumps(payload))
    log(f"Added to Brevo (List {list_id}): {contact.get('email','(no email)')} ({resp.status_code})")

# ---------------------------------------------
# SCRAPER CORE
# ---------------------------------------------
def run_scraper(selected_subterms, zipcode, radius):
    scraper_logs.clear()
    seen_emails.clear()
    log("🚀 Scraper started.")

    start = time.time()
    uploaded = 0

    # minute countdown pings
    next_ping = 60

    for subterm in selected_subterms:
        if time.time() - start >= TIMEOUT_SECONDS:
            log("⏱ Timeout reached — stopping.")
            break
        if uploaded >= MIN_LEADS:
            log("✅ Minimum leads reached — stopping.")
            break

        log(f"🔎 Searching {subterm} near {zipcode} ({radius} mi radius)...")
        businesses = get_businesses_from_google(subterm, zipcode, radius, start)

        for biz in businesses:
            if time.time() - start >= TIMEOUT_SECONDS:
                log("⏱ Timeout during processing — finishing up.")
                break
            if uploaded >= MIN_LEADS:
                break

            name = (biz.get("name") or "Unknown").strip()
            website = biz.get("website", "")
            phone = biz.get("phone", "")

            email = find_email_on_website(website)

            # Deduplicate on email if present, else dedupe on name+phone
            key = email.lower() if email else f"{name}|{phone}"
            if key in seen_emails:
                continue
            seen_emails.add(key)

            if email:
                add_to_brevo({"name": name, "email": email, "phone": phone, "website": website}, 3)
                uploaded += 1
                log(f"✅ {name} ({email}) → List 3")
            elif phone:
                add_to_brevo({"name": name, "phone": phone, "website": website}, 5)
                uploaded += 1
                log(f"📇 {name} (No Email) → List 5")
            else:
                log(f"❌ Skipped {name} — no email or phone.")

            # minute countdown logs
            elapsed = int(time.time() - start)
            if elapsed >= next_ping and elapsed < TIMEOUT_SECONDS:
                remaining = TIMEOUT_SECONDS - elapsed
                mins = max(0, remaining // 60)
                secs = remaining % 60
                log(f"⏱ Timeout countdown: {mins}:{secs:02d} remaining...")
                next_ping += 60

        if time.time() - start >= TIMEOUT_SECONDS or uploaded >= MIN_LEADS:
            break

    log(f"🎯 Finished — {uploaded} contacts processed.")

# ---------------------------------------------
# PAGES
# ---------------------------------------------
def category_html():
    # Build grouped checkboxes with header toggles
    sections = []
    for group, items in CATEGORY_GROUPS.items():
        section = f"""
        <div class="cat-group">
          <div class="cat-title" onclick="toggleGroup(this)">{group}</div>
          <div class="cat-items">
        """
        for sub in items:
            section += f"""
            <label class="chk">
              <input type="checkbox" name="subcategory" value="{sub}">
              <span>{sub}</span>
            </label>
            """
        section += "</div></div>"
        sections.append(section)
    return "\n".join(sections)

@app.route("/")
def home():
    html = f"""
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Business Lead Scraper</title>
<style>
  body {{ background:#000; color:#00aaff; font-family:Consolas, monospace; margin:0; }}
  .nav {{ padding:14px 18px; background:#050505; border-bottom:1px solid #00bfff; display:flex; gap:16px; }}
  .nav a {{ color:#00bfff; text-decoration:none; }}
  .wrap {{ max-width:1100px; margin:0 auto; padding:24px; }}
  h1 {{ color:#00bfff; margin:10px 0 18px; text-align:center; }}
  .row {{ display:flex; flex-wrap:wrap; gap:12px; justify-content:center; margin-bottom:16px; }}
  .inp {{ background:#0b0b0b; color:#00bfff; border:1px solid #00bfff; border-radius:8px; padding:12px 14px; min-width:220px; }}
  .btn {{ background:#00bfff; color:#000; border:none; font-weight:700; padding:12px 20px; border-radius:10px; cursor:pointer; }}
  .btn:hover {{ background:#0090da; }}
  .grid {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap:14px; }}
  .card {{ background:#0a0a0a; border:1px solid #00bfff; border-radius:12px; padding:14px; }}
  .cat-title {{ color:#00bfff; font-weight:700; margin-bottom:8px; cursor:pointer; }}
  .cat-items {{ display:flex; flex-direction:column; gap:6px; }}
  .chk input {{ margin-right:8px; }}
  @media (max-width:640px) {{
    .inp {{ min-width:140px; flex:1; }}
  }}
</style>
<script>
  function toggleGroup(el) {{
    const box = el.nextElementSibling;
    const checks = box.querySelectorAll('input[type=checkbox]');
    const allChecked = Array.from(checks).every(c => c.checked);
    checks.forEach(c => c.checked = !allChecked);
  }}
  function runScraper() {{
    const zip = document.getElementById('zip').value.trim();
    const radius = document.getElementById('radius').value.trim();
    const sel = Array.from(document.querySelectorAll('input[name=subcategory]:checked')).map(x => x.value);
    const params = new URLSearchParams();
    params.set('zipcode', zip || '23005');
    params.set('radius', radius || '30');
    if (sel.length) sel.forEach(s => params.append('subcategory', s));
    else params.append('subcategory', 'Landscaping');
    window.location = '/run?' + params.toString();
  }}
</script>
</head>
<body>
  <div class="nav">
    <a href="/">Home</a>
    <a href="/previous">Previous Runs</a>
    <a href="/about">About</a>
    <a href="/help">Help</a>
  </div>
  <div class="wrap">
    <h1>Business Lead Scraper</h1>
    <div class="row">
      <input id="zip" class="inp" placeholder="ZIP Code (e.g., 23005)">
      <input id="radius" class="inp" type="number" min="1" max="100" placeholder="Radius in miles (e.g., 30)">
    </div>
    <div class="grid">
      {category_html()}
    </div>
    <div style="text-align:center; margin-top:18px;">
      <button class="btn" onclick="runScraper()">Run Scraper</button>
    </div>
  </div>
</body>
</html>
"""
    return render_template_string(html)

@app.route("/run")
def run():
    zipcode = request.args.get("zipcode", "23005")
    radius = request.args.get("radius", "30")
    selected = request.args.getlist("subcategory")
    if not selected:
        selected = ["Landscaping"]

    threading.Thread(target=run_scraper, args=(selected, zipcode, radius), daemon=True).start()

    # Quick splash then to logs
    return render_template_string("""
<!doctype html>
<html><head>
<meta http-equiv="refresh" content="2;url=/logs">
<style>
  body{background:#000;color:#00aaff;font-family:Consolas;text-align:center;padding-top:50px;}
</style>
</head>
<body>
  <h2>🚀 Scraper starting…</h2>
  <p>Taking you to logs…</p>
</body></html>
""")

@app.route("/logs")
def logs():
    return render_template_string("""
<!doctype html>
<html>
<head>
<meta http-equiv="refresh" content="3">
<style>
  body{background:#000;color:#00aaff;font-family:Consolas;margin:0;}
  .nav{padding:14px 18px; background:#050505; border-bottom:1px solid #00bfff;}
  .nav a{color:#00bfff;text-decoration:none;}
  .wrap{max-width:1100px;margin:0 auto;padding:24px;}
  .box{border:1px solid #00bfff;border-radius:10px;background:#0a0a0a;height:460px;overflow:auto;padding:16px;}
</style>
</head>
<body>
  <div class="nav"><a href="/">← Back to Search</a></div>
  <div class="wrap">
    <h2>Live Logs</h2>
    <div class="box">
      {% for line in logs %}
        <div>{{ line }}</div>
      {% endfor %}
    </div>
  </div>
</body>
</html>
""", logs=scraper_logs)

@app.route("/previous")
def previous():
    return render_template_string("""
<!doctype html><html><head>
<style>
  body{background:#000;color:#00aaff;font-family:Consolas;margin:0;}
  .nav{padding:14px 18px; background:#050505; border-bottom:1px solid #00bfff;}
  .nav a{color:#00bfff;text-decoration:none;}
  .wrap{max-width:1100px;margin:0 auto;padding:24px;text-align:center;}
</style>
</head><body>
  <div class="nav"><a href="/">Home</a></div>
  <div class="wrap"><h2>Previous Runs (coming soon)</h2></div>
</body></html>
""")

@app.route("/about")
def about():
    return render_template_string("""
<!doctype html><html><head>
<style>
  body{background:#000;color:#00aaff;font-family:Consolas;margin:0;}
  .nav{padding:14px 18px; background:#050505; border-bottom:1px solid #00bfff;}
  .nav a{color:#00bfff;text-decoration:none;}
  .wrap{max-width:1100px;margin:0 auto;padding:24px;}
</style>
</head><body>
  <div class="nav"><a href="/">Home</a></div>
  <div class="wrap">
    <h2>About</h2>
    <p>Business Lead Scraper finds local businesses by category near a ZIP+radius and uploads contacts directly to Brevo.
    Emails → List 3. Phone/no email → List 5. 3-minute hard timeout, 30-lead target.</p>
  </div>
</body></html>
""")

@app.route("/help")
def help_page():
    return render_template_string("""
<!doctype html><html><head>
<style>
  body{background:#000;color:#00aaff;font-family:Consolas;margin:0;}
  .nav{padding:14px 18px; background:#050505; border-bottom:1px solid #00bfff;}
  .nav a{color:#00bfff;text-decoration:none;}
  .wrap{max-width:1100px;margin:0 auto;padding:24px;}
  ul{{line-height:1.7}}
</style>
</head><body>
  <div class="nav"><a href="/">Home</a></div>
  <div class="wrap">
    <h2>Help</h2>
    <ul>
      <li>Enter ZIP and radius in miles.</li>
      <li>Click category headers to select/deselect all subcategories, or pick individual ones.</li>
      <li>Click Run Scraper to begin. You’ll be sent to Logs automatically.</li>
      <li>Scraper stops at 3 minutes or when 30+ leads are uploaded.</li>
      <li>Brevo List 3: has email. List 5: phone only.</li>
    </ul>
  </div>
</body></html>
""")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)

