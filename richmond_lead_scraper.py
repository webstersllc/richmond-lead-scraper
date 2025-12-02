import os
import requests
import json
from flask import Flask, render_template_string, request, jsonify
from datetime import datetime
import time
import re
import pandas as pd

# --------------------------------------------------------------------
# Environment variables
# --------------------------------------------------------------------
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
BREVO_API_KEY = os.getenv("BREVO_API_KEY")

if not GOOGLE_API_KEY or not BREVO_API_KEY:
    raise ValueError("Missing GOOGLE_API_KEY or BREVO_API_KEY")

# --------------------------------------------------------------------
# Flask app + globals
# --------------------------------------------------------------------
app = Flask(__name__)
scraper_logs = []
seen_emails = set()
scraper_in_progress = False  # prevent multiple runs at once

# --------------------------------------------------------------------
# Logging helper
# --------------------------------------------------------------------
def log_message(message: str):
    timestamp = datetime.now().strftime("%H:%M:%S")
    entry = f"[{timestamp}] {message}"
    print(entry)
    scraper_logs.append(entry)
    if len(scraper_logs) > 400:
        scraper_logs.pop(0)

# --------------------------------------------------------------------
# Phone helpers
# --------------------------------------------------------------------
PHONE_PATTERN = re.compile(r"(?:\+?1[\s\-\.]?)?\(?\d{3}\)?[\s\-\.]?\d{3}[\s\-\.]?\d{4}")

def normalize_phone(raw: str) -> str:
    """Normalize US phones to +1-AAA-BBB-CCCC when possible."""
    if not raw:
        return ""
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 10:
        return f"+1-{digits[0:3]}-{digits[3:6]}-{digits[6:10]}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+1-{digits[1:4]}-{digits[4:7]}-{digits[7:11]}"
    return ""

# --------------------------------------------------------------------
# Example-email blacklist (your CSV list)
# --------------------------------------------------------------------
BAD_EMAILS = {
    "johndoe@example.com",
    "janedoe@example.com",
    "yourname@example.com",
    "info@example.com",
    "contact@example.com",
    "hello@example.com",
    "admin@example.com",
    "support@example.com",
    "user@example.com",
    "person@example.com",
    "firstname.lastname@example.com",
    "name@company.com",
    "me@example.com",
    "team@example.com",
    "demo@example.com",
    "test@example.com",
    "sample@example.com",
    "email@example.com",
    "myemail@example.com",
    "user123@example.com",
    "contactus@example.com",
    "info@company.com",
    "hello@brand.com",
    "support@service.com",
    "admin@business.com",
    "sales@example.com",
    "marketing@example.com",
    "office@example.com",
    "customerservice@example.com",
    "feedback@example.com",
    "help@example.com",
    "team@company.com",
    "inquiries@example.com",
    "press@example.com",
    "hr@example.com",
    "careers@example.com",
    "owner@example.com",
    "staff@example.com",
    "account@example.com",
    "service@example.com",
    "billing@example.com",
    "media@example.com",
    "info@domain.com",
    "contact@domain.com",
    "support@domain.com",
    "hello@domain.com",
    "demo@domain.com",
    "test@domain.com",
    "name@domain.com",
    "user@domain.com",
    "email@domain.com",
    "feedback@domain.com",
    "info@website.com",
    "contact@website.com",
    "support@website.com",
    "hello@website.com",
    "sales@website.com",
    "name@website.com",
    "user@website.com",
    "myname@website.com",
    "yourname@website.com",
    "example@website.com",
    "test@website.com",
    "jane.doe@website.com",
    "john.doe@website.com",
    "firstname@website.com",
    "lastname@website.com",
    "contactme@website.com",
    "sayhello@website.com",
    "reachout@website.com",
    "contactform@website.com",
    "info@mysite.com",
    "admin@mysite.com",
    "help@mysite.com",
    "demo@mysite.com",
    "user@mysite.com",
    "support@mysite.com",
    "hello@mysite.com",
    "info@email.com",
    "contact@email.com",
    "example@email.com",
    "user@email.com",
    "name@email.com",
    "test@email.com",
    "info@sample.com",
    "contact@sample.com",
    "user@sample.com",
    "hello@sample.com",
    "name@sample.com",
    "demo@sample.com",
    "contact@demo.com",
    "info@demo.com",
    "user@demo.com",
    "hello@demo.com",
    "name@demo.com",
    "test@demo.com",
    "someone@example.com",
    "you@example.com",
    "your.email@example.com",
    "contactperson@example.com",
}

BAD_EMAIL_SUBSTRINGS = [
    "wixpress",
    "sentry",
    "schema.org",
]

def is_bad_email(email: str) -> bool:
    if not email:
        return True
    lower = email.strip().lower()
    if lower in BAD_EMAILS:
        return True
    for bad in BAD_EMAIL_SUBSTRINGS:
        if bad in lower:
            return True
    return False

# --------------------------------------------------------------------
# Google Places lookup (businesses + phone from details)
# --------------------------------------------------------------------
def get_businesses_from_google(category, zipcode, radius_miles):
    radius_meters = int(radius_miles) * 1609
    query = f"{category} near {zipcode}"
    url = (
        "https://maps.googleapis.com/maps/api/place/textsearch/json"
        f"?query={requests.utils.quote(query)}&radius={radius_meters}&key={GOOGLE_API_KEY}"
    )
    log_message(f"🔎 Searching {category} near {zipcode} ({radius_miles} mi radius)…")
    resp = requests.get(url, timeout=12)
    data = resp.json()
    results = data.get("results", [])

    # Simple pagination – up to ~60 results total if available
    next_page = data.get("next_page_token")
    page_count = 1
    while next_page and page_count < 3:
        time.sleep(2)
        resp = requests.get(
            "https://maps.googleapis.com/maps/api/place/textsearch/json",
            params={"pagetoken": next_page, "key": GOOGLE_API_KEY},
            timeout=12
        )
        more = resp.json()
        results.extend(more.get("results", []))
        next_page = more.get("next_page_token")
        page_count += 1

    log_message(f"📍 Retrieved {len(results)} {category} results total.")

    data_out = []
    for r in results:
        name = r.get("name", "Unknown Business")
        pid = r.get("place_id")
        if not pid:
            data_out.append({"name": name, "website": "", "phone": ""})
            continue

        deturl = (
            "https://maps.googleapis.com/maps/api/place/details/json"
            f"?place_id={pid}&fields=name,website,formatted_phone_number,international_phone_number&key={GOOGLE_API_KEY}"
        )
        try:
            det = requests.get(deturl, timeout=12).json().get("result", {})
        except Exception:
            det = {}

        g_phone_raw = det.get("international_phone_number") or det.get("formatted_phone_number") or ""
        g_phone = normalize_phone(g_phone_raw)

        data_out.append({
            "name": name,
            "website": det.get("website", ""),
            "phone": g_phone
        })
        time.sleep(0.15)

    return data_out

# --------------------------------------------------------------------
# Email scraping
# --------------------------------------------------------------------
def find_email_on_website(url):
    if not url:
        return ""
    try:
        r = requests.get(url, timeout=6)
        emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", r.text)
        for e in emails:
            if is_bad_email(e):
                continue
            return e
    except Exception as e:
        log_message(f"Error scanning {url}: {e}")
    return ""

# --------------------------------------------------------------------
# Owner + phone scraping
# --------------------------------------------------------------------
def _extract_phone_from_html(html: str) -> str:
    # Prioritize tel: links
    tel_links = re.findall(r'href=["\']tel:([^"\']+)["\']', html, flags=re.IGNORECASE)
    for raw in tel_links:
        norm = normalize_phone(raw)
        if norm:
            return norm

    # Fall back to text scanning
    txt = re.sub(r"<[^>]*>", " ", html)
    txt = re.sub(r"\s+", " ", txt)
    m = PHONE_PATTERN.search(txt)
    if m:
        norm = normalize_phone(m.group(0))
        if norm:
            return norm
    return ""

def find_owner_name_and_phone(url):
    if not url:
        return "", ""

    def fetch(u):
        try:
            return requests.get(u, timeout=8)
        except Exception:
            return None

    # main page
    r = fetch(url)
    if not r or not r.text:
        return "", ""

    html_main = r.text
    phone = _extract_phone_from_html(html_main)

    # try common subpages if no phone found yet
    extra_html = ""
    if not phone:
        for slug in ["contact", "contact-us", "about", "about-us", "team", "our-team"]:
            test = url.rstrip("/") + f"/{slug}"
            rr = fetch(test)
            if rr and rr.text:
                extra_html += " " + rr.text
                phone = _extract_phone_from_html(rr.text)
                if phone:
                    break

    combined = html_main + extra_html
    combined_txt = re.sub(r"<[^>]*>", " ", combined)
    combined_txt = re.sub(r"\s+", " ", combined_txt)

    owner = ""
    for line in combined_txt.split("."):
        if any(k in line.lower() for k in ["owner", "ceo", "founder", "manager", "director", "president"]):
            nm = re.search(r"\b([A-Z][a-z]+ [A-Z][a-z]+)\b", line)
            if nm:
                owner = nm.group(1)
                break

    return owner, phone

# --------------------------------------------------------------------
# Brevo integration
# --------------------------------------------------------------------
def add_to_brevo(contact, has_email=True):
    url = "https://api.brevo.com/v3/contacts"
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "api-key": BREVO_API_KEY
    }

    # For no-email records, create a placeholder address keyed on business name
    email_to_use = contact.get("email") if has_email else f"{re.sub(r'[^a-z0-9]', '', contact['name'].lower())}@placeholder.com"

    payload = {
        "email": email_to_use,
        "attributes": {
            "FIRSTNAME": contact.get("owner_name") or contact.get("name"),
            "COMPANY": contact.get("name"),
            "PHONE": normalize_phone(contact.get("phone", "")),
            "WEBSITE": contact.get("website", "")
        },
        "listIds": [3 if has_email else 5]
    }
    r = requests.post(url, headers=headers, data=json.dumps(payload))
    log_message(
        f"Added to Brevo (List {'3' if has_email else '5'}): {contact.get('email', 'no email')} ({r.status_code})"
    )

# --------------------------------------------------------------------
# Scraper process
# --------------------------------------------------------------------
def run_scraper_process(categories, zipcode, radius):
    global scraper_in_progress
    if scraper_in_progress:
        log_message("⚠️ A scraper is already running. Please wait for it to finish.")
        return

    scraper_in_progress = True
    scraper_logs.clear()
    seen_emails.clear()
    log_message("🚀 Scraper started.")

    results = []
    uploaded = 0

    # Collect businesses from all selected categories
    for c in categories:
        results.extend(get_businesses_from_google(c, zipcode, radius))

    # cap at 400 businesses per run for sanity
    for biz in results[:400]:
        email = find_email_on_website(biz["website"])
        if email and is_bad_email(email):
            log_message(f"🚫 Skipping example/placeholder email: {email}")
            email = ""

        owner, scraped_phone = find_owner_name_and_phone(biz["website"])
        best_phone = normalize_phone(scraped_phone) or normalize_phone(biz.get("phone", "")) or ""

        contact = {
            "name": biz["name"],
            "phone": best_phone,
            "website": biz["website"],
            "email": email,
            "owner_name": owner
        }

        if email:
            if email in seen_emails:
                log_message(f"⚠️ Duplicate skipped before upload: {email}")
                continue
            add_to_brevo(contact, has_email=True)
            seen_emails.add(email)
            uploaded += 1
            log_message(f"✅ {biz['name']} ({email}) → List 3")
        else:
            add_to_brevo(contact, has_email=False)
            uploaded += 1
            log_message(f"📇 {biz['name']} (No Email) → List 5")

        if best_phone:
            log_message(f"📞 Phone for {biz['name']}: {best_phone}")

        time.sleep(0.5)

    # Save raw results to Excel
    os.makedirs("runs", exist_ok=True)
    fname = f"runs/run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    try:
        pd.DataFrame(results).to_excel(fname, index=False)
        log_message(f"📁 Saved as {fname}")
    except Exception as e:
        log_message(f"⚠️ Could not save Excel file: {e}")

    log_message(f"🎯 Finished — {uploaded} uploaded.")
    scraper_in_progress = False

# --------------------------------------------------------------------
# Base styles for all pages
# --------------------------------------------------------------------
BASE_STYLE = """
<style>
body{
  background:#000;
  color:#00bfff;
  font-family:Consolas,monospace;
  text-align:center;
  padding:20px;
}
h1{color:#00bfff}
h2{color:#0099ff}
button,input[type=text]{
  padding:10px;
  margin:5px;
  border-radius:6px;
  font-weight:bold;
}
.navbar{
  margin-bottom:20px;
}
.navbar a{
  color:#00bfff;
  margin:0 10px;
  text-decoration:none;
}
#log-box{
  width:80%;
  margin:20px auto;
  text-align:left;
  height:400px;
  overflow-y:auto;
  background:#0a0a0a;
  border:1px solid #00bfff;
  border-radius:10px;
  padding:20px;
}
.grid{
  display:flex;
  flex-wrap:wrap;
  justify-content:center;
  gap:15px;
  margin-top:10px;
}
.group{
  border:1px solid #00bfff;
  border-radius:10px;
  padding:10px;
  width:250px;
}
.group h3{
  color:#00bfff;
  cursor:pointer;
  text-decoration:underline;
}
</style>
"""

# --------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------
@app.route("/")
def home():
    grouped = {
        "Food & Drink": [
            "Restaurants","Bars & Clubs","Coffee Shops","Bakeries",
            "Breweries","Cafes","Juice Bars"
        ],
        "Retail & Shopping": [
            "Retail Stores","Boutiques","Clothing Stores","Gift Shops",
            "Bookstores","Home Goods Stores"
        ],
        "Beauty & Wellness": [
            "Salons","Barbers","Spas","Massage Therapy","Nail Salons"
        ],
        "Fitness & Recreation": [
            "Gyms","Yoga Studios","Martial Arts","CrossFit","Dance Studios"
        ],
        "Home Services": [
            "HVAC","Plumbing","Electricians","Landscaping",
            "Cleaning Services","Painting","Roofing","Pest Control"
        ],
        "Auto Services": [
            "Auto Repair","Car Wash","Tire Shops","Car Dealerships","Detailing"
        ],
        "Insurance & Finance": [
            "Insurance Agencies","Banks","Credit Unions","Financial Advisors"
        ],
        "Events & Entertainment": [
            "Event Venues","Wedding Planners","Catering",
            "Escape Rooms","Putt Putt","Bowling Alleys"
        ],
        "Construction & Real Estate": [
            "Construction Companies","Contractors","Real Estate Agencies","Home Builders"
        ],
        "Health & Medical": [
            "Dentists","Doctors","Chiropractors","Physical Therapy","Veterinarians"
        ],
        "Pets": [
            "Pet Groomers","Pet Boarding","Pet Stores"
        ],
        "Education & Childcare": [
            "Daycares","Private Schools","Tutoring Centers","Learning Centers"
        ],
        "Professional Services": [
            "Law Firms","Accountants","Consulting Firms"
        ],
        "Community & Nonprofits": [
            "Churches","Nonprofits","Community Centers"
        ]
    }

    html = f"""{BASE_STYLE}
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
"""
    for g, cats in grouped.items():
        html += f"<div class='group'><h3 onclick=\"toggleGroup('{g}')\">{g}</h3>"
        for c in cats:
            html += f"<label><input type='checkbox' name='categories' value='{c}'> {c}</label><br>"
        html += "</div>"
    html += """
  </div><br>
  ZIP Code: <input type='text' name='zipcode' required>
  Radius (mi): <input type='text' name='radius' required value='10'><br><br>
  <button type='submit'>Start Search</button>
</form>
<script>
function toggleGroup(name){
  const groups = document.querySelectorAll('.group');
  groups.forEach(div => {
    const h = div.querySelector('h3');
    if(h && h.textContent.trim() === name){
      const boxes = div.querySelectorAll('input[type="checkbox"]');
      const allChecked = [...boxes].every(b => b.checked);
      boxes.forEach(b => b.checked = !allChecked);
    }
  });
}
</script>
"""
    return render_template_string(html)

@app.route("/run")
def run_scraper():
    cats = request.args.getlist("categories")
    zipc = request.args.get("zipcode", "23220")
    rad = request.args.get("radius", "10")

    import threading
    threading.Thread(target=run_scraper_process, args=(cats, zipc, rad)).start()

    html = """
<style>
body{
  background:#000;
  color:#00bfff;
  font-family:Consolas,monospace;
  text-align:center;
  padding:20px;
}
h1{color:#00bfff}
h2{color:#0099ff}
#log-box{
  width:80%;
  margin:20px auto;
  text-align:left;
  height:400px;
  overflow-y:auto;
  background:#0a0a0a;
  border:1px solid #00bfff;
  border-radius:10px;
  padding:20px;
}
.navbar a{
  color:#00bfff;
  margin:0 10px;
  text-decoration:none;
}
</style>
<div class='navbar'>
  <a href='/'>Back</a> |
  <a href='/previous'>Previous Runs</a> |
  <a href='/about'>About</a> |
  <a href='/help'>Help</a>
</div>
<h1>Business Lead Scraper</h1>
<h2>Running… Logs below</h2>
<div id='log-box'></div>
<script>
async function fetchLogs(){
  const r = await fetch('/logs');
  const d = await r.json();
  const box = document.getElementById('log-box');
  box.innerHTML = d.logs.map(l => '<div>' + l + '</div>').join('');
  box.scrollTop = box.scrollHeight;
}
setInterval(fetchLogs, 2000);
</script>
"""
    return render_template_string(html)

@app.route("/previous")
def previous():
    files = os.listdir("runs") if os.path.exists("runs") else []
    links = "".join(f"<li><a href='/runs/{f}'>{f}</a></li>" for f in files)
    return render_template_string(
        f"""{BASE_STYLE}
<div class='navbar'><a href='/'>Home</a></div>
<h1>Previous Runs</h1>
<ul>{links}</ul>
"""
    )

@app.route("/about")
def about():
    return render_template_string(
        f"""{BASE_STYLE}
<div class='navbar'><a href='/'>Home</a></div>
<h1>About</h1>
<p>Business Lead Scraper finds local businesses via Google Places, scrapes websites for emails and phone numbers, 
and uploads contacts into your Brevo lists:
List 3 for leads with emails, List 5 for leads with phone only.</p>
"""
    )

@app.route("/help")
def help_page():
    return render_template_string(
        f"""{BASE_STYLE}
<div class='navbar'><a href='/'>Home</a></div>
<h1>Help</h1>
<p>
1 Select one or more business categories.<br>
2 Enter a ZIP code and radius in miles.<br>
3 Click "Start Search".<br>
4 Watch live logs as Bertha scrapes, filters bad/example emails, finds phones, 
   and sends leads to Brevo.<br>
5 Download raw XLSX files under "Previous Runs".
</p>
"""
    )

@app.route("/logs")
def logs():
    return jsonify({"logs": scraper_logs})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)