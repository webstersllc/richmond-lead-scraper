import os
import requests
import json
from flask import Flask, render_template_string, request, jsonify, send_from_directory
from datetime import datetime
import time
import re
import pandas as pd

# ---------------------------------------------------------
# Environment variables
# ---------------------------------------------------------
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
BREVO_API_KEY = os.getenv("BREVO_API_KEY")

if not GOOGLE_API_KEY or not BREVO_API_KEY:
    raise ValueError("Missing GOOGLE_API_KEY or BREVO_API_KEY")

app = Flask(__name__)

scraper_logs = []
seen_emails = set()
scraper_in_progress = False  # prevent multiple runs

# ---------------------------------------------------------
# Bad / placeholder emails to ignore (Bertha v1)
# ---------------------------------------------------------
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
    # generic examples that sometimes pop up
    "user@domain.com",
    "user@website.com",
}

# quick pattern-based ignores (domains etc)
BAD_EMAIL_PATTERNS = [
    "example.com",
    "wixpress.com",
    "schema.org",
    "sentry.io",
]

# ---------------------------------------------------------
# Logging helper
# ---------------------------------------------------------
def log_message(message: str):
    timestamp = datetime.now().strftime("%H:%M:%S")
    entry = f"[{timestamp}] {message}"
    print(entry)
    scraper_logs.append(entry)
    if len(scraper_logs) > 400:
        scraper_logs.pop(0)

# ---------------------------------------------------------
# Phone normalization for Brevo
# ---------------------------------------------------------
def normalize_phone(phone: str) -> str:
    """
    Normalize phone for Brevo's sms field.
    - Strip non-digits
    - If 10 digits, assume US and prefix +1
    - If 11 digits and starts with 1, prefix +
    - Otherwise return original stripped phone
    """
    if not phone:
        return ""
    digits = re.sub(r"\D", "", phone)
    if len(digits) == 10:
        return "+1" + digits
    if len(digits) == 11 and digits.startswith("1"):
        return "+" + digits
    # if it already looks like +18045551212, leave as is
    if phone.strip().startswith("+"):
        return phone.strip()
    return phone.strip()

# ---------------------------------------------------------
# Google Places search
# ---------------------------------------------------------
def get_businesses_from_google(category: str, zipcode: str, radius_miles: str):
    radius_meters = int(radius_miles) * 1609
    query = f"{category} near {zipcode}"
    url = (
        "https://maps.googleapis.com/maps/api/place/textsearch/json"
        f"?query={query}&radius={radius_meters}&key={GOOGLE_API_KEY}"
    )
    log_message(f"🔎 Searching {category} near {zipcode} ({radius_miles} mi radius)…")
    resp = requests.get(url)
    data = resp.json()
    results = data.get("results", [])
    log_message(f"📍 Retrieved {len(results)} {category} results total.")

    businesses = []
    for r in results:
        name = r.get("name", "Unknown Business")
        place_id = r.get("place_id")
        details_url = (
            "https://maps.googleapis.com/maps/api/place/details/json"
            f"?place_id={place_id}&fields=name,website,formatted_phone_number&key={GOOGLE_API_KEY}"
        )
        d = requests.get(details_url).json().get("result", {})
        businesses.append(
            {
                "name": name,
                "website": d.get("website", ""),
                "phone": d.get("formatted_phone_number", ""),
                "category": category,
            }
        )
        time.sleep(0.2)
    return businesses

# ---------------------------------------------------------
# Email scraping from a website
# ---------------------------------------------------------
def is_bad_email(email: str) -> bool:
    e = email.strip().lower()
    if e in BAD_EMAILS:
        return True
    for pat in BAD_EMAIL_PATTERNS:
        if pat in e:
            return True
    return False

def find_email_on_website(url: str) -> str:
    if not url:
        return ""
    try:
        r = requests.get(url, timeout=6)
        emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", r.text)
        for e in emails:
            e_norm = e.strip()
            if is_bad_email(e_norm):
                continue
            return e_norm
    except Exception as ex:
        log_message(f"⚠️ Error scanning {url}: {ex}")
    return ""

# ---------------------------------------------------------
# Owner / phone extraction from HTML
# ---------------------------------------------------------
def find_owner_name_and_phone(url: str):
    if not url:
        return "", ""
    try:
        r = requests.get(url, timeout=6)
        txt = re.sub(r"<[^>]*>", " ", r.text)
        txt = re.sub(r"\s+", " ", txt)

        owner_name = ""
        phone = ""

        # try to find owner-style line
        for line in txt.split("."):
            if any(k in line.lower() for k in ["owner", "ceo", "founder", "manager", "director", "president"]):
                nm = re.search(r"\b([A-Z][a-z]+ [A-Z][a-z]+)\b", line)
                if nm:
                    owner_name = nm.group(1)
                    break

        # generic phone search
        ph_match = re.search(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", txt)
        if ph_match:
            phone = ph_match.group(0)

        return owner_name, phone
    except Exception as ex:
        log_message(f"⚠️ Error parsing {url}: {ex}")
        return "", ""

# ---------------------------------------------------------
# Brevo contact creation (email and no-email paths)
# ---------------------------------------------------------
def add_to_brevo(contact: dict, has_email: bool = True):
    """
    Sends contact to Brevo.
    - If has_email True: real email used, list 3
    - If has_email False: placeholder email, list 5
    - Always tries to push phone both as:
        • attributes['PHONE']  (for your custom attribute)
        • sms                  (for Brevo's phone/SMS field)
    """
    url = "https://api.brevo.com/v3/contacts"
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "api-key": BREVO_API_KEY,
    }

    raw_phone = (contact.get("phone") or "").strip()
    phone_for_sms = normalize_phone(raw_phone)

    # Email logic
    if has_email:
        email_value = contact.get("email")
    else:
        # generate a deterministic placeholder based on business name
        base = (contact.get("name") or "unknown").replace(" ", "").lower()
        email_value = f"{base}@placeholder.com"

    payload = {
        "email": email_value,
        "attributes": {
            # FIRSTNAME doubles as contact name if we don't have a personal name
            "FIRSTNAME": contact.get("owner_name") or contact.get("name"),
            "COMPANY": contact.get("name"),
            "PHONE": raw_phone,  # your custom PHONE attribute
            "WEBSITE": contact.get("website", ""),
        },
        "listIds": [3 if has_email else 5],
    }

    # Only include sms if we got something non-empty
    if phone_for_sms:
        payload["sms"] = phone_for_sms

    try:
        r = requests.post(url, headers=headers, data=json.dumps(payload))
        list_id = 3 if has_email else 5
        log_message(
            f"Added to Brevo (List {list_id}): {email_value} | phone: {raw_phone} "
            f"→ status {r.status_code}"
        )
    except Exception as ex:
        log_message(f"⚠️ Error sending to Brevo for {email_value}: {ex}")

# ---------------------------------------------------------
# Scraper process
# ---------------------------------------------------------
def run_scraper_process(categories, zipcode, radius):
    global scraper_in_progress
    if scraper_in_progress:
        log_message("⚠️ A scraper is already running. Please wait for it to finish.")
        return
    scraper_in_progress = True

    scraper_logs.clear()
    seen_emails.clear()
    log_message("🚀 Scraper started.")

    all_results = []
    uploaded = 0

    # collect businesses for all selected categories
    for c in categories:
        all_results.extend(get_businesses_from_google(c, zipcode, radius))

    # cap the processing to 400 to keep it reasonable
    for biz in all_results[:400]:
        website = biz.get("website", "")
        base_phone = biz.get("phone", "")

        email = find_email_on_website(website)
        owner, phone_from_page = find_owner_name_and_phone(website)

        final_phone = phone_from_page or base_phone

        contact = {
            "name": biz.get("name"),
            "phone": final_phone,
            "website": website,
            "email": email,
            "owner_name": owner,
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
            # no email – send to list 5 using placeholder email
            add_to_brevo(contact, has_email=False)
            uploaded += 1
            log_message(f"📇 {biz['name']} (No Email) → List 5")

        time.sleep(0.5)

    # Save Excel of raw business results (not just uploaded)
    os.makedirs("runs", exist_ok=True)
    fname = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    full_path = os.path.join("runs", fname)
    try:
        pd.DataFrame(all_results).to_excel(full_path, index=False)
        log_message(f"📁 Saved as runs/{fname}")
    except Exception as ex:
        log_message(f"⚠️ Failed saving Excel run file: {ex}")

    log_message(f"🎯 Finished — {uploaded} total contacts uploaded.")
    scraper_in_progress = False

# ---------------------------------------------------------
# Base style shared across pages
# ---------------------------------------------------------
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
ul{list-style:none;padding:0}
li{margin:5px 0}
</style>
"""

# ---------------------------------------------------------
# Routes
# ---------------------------------------------------------
@app.route("/")
def home():
    grouped = {
        "Food & Drink": [
            "Restaurants", "Bars & Clubs", "Coffee Shops", "Bakeries",
            "Breweries", "Cafes", "Juice Bars"
        ],
        "Retail & Shopping": [
            "Retail Stores", "Boutiques", "Clothing Stores", "Gift Shops",
            "Bookstores", "Home Goods Stores"
        ],
        "Beauty & Wellness": [
            "Salons", "Barbers", "Spas", "Massage Therapy", "Nail Salons"
        ],
        "Fitness & Recreation": [
            "Gyms", "Yoga Studios", "Martial Arts", "CrossFit", "Dance Studios"
        ],
        "Home Services": [
            "HVAC", "Plumbing", "Electricians", "Landscaping",
            "Cleaning Services", "Painting", "Roofing", "Pest Control"
        ],
        "Auto Services": [
            "Auto Repair", "Car Wash", "Tire Shops",
            "Car Dealerships", "Detailing"
        ],
        "Insurance & Finance": [
            "Insurance Agencies", "Banks", "Credit Unions", "Financial Advisors"
        ],
        "Events & Entertainment": [
            "Event Venues", "Wedding Planners", "Catering",
            "Escape Rooms", "Putt Putt", "Bowling Alleys"
        ],
        "Construction & Real Estate": [
            "Construction Companies", "Contractors",
            "Real Estate Agencies", "Home Builders"
        ],
        "Health & Medical": [
            "Dentists", "Doctors", "Chiropractors",
            "Physical Therapy", "Veterinarians"
        ],
        "Pets": [
            "Pet Groomers", "Pet Boarding", "Pet Stores"
        ],
        "Education & Childcare": [
            "Daycares", "Private Schools", "Tutoring Centers", "Learning Centers"
        ],
        "Professional Services": [
            "Law Firms", "Accountants", "Consulting Firms"
        ],
        "Community & Nonprofits": [
            "Churches", "Nonprofits", "Community Centers"
        ],
    }

    html = BASE_STYLE + """
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
    # groups and checkboxes
    for g, cats in grouped.items():
        html += f"<div class='group'><h3 onclick=\"toggleGroup('{g}')\">{g}</h3>"
        for c in cats:
            html += (
                f"<label><input type='checkbox' name='categories' "
                f"value='{c}'> {c}</label><br>"
            )
        html += "</div>"
    html += """
  </div>
  <br>
  ZIP Code:
  <input type='text' name='zipcode' required>
  Radius (mi):
  <input type='text' name='radius' required value='10'>
  <br><br>
  <button type='submit'>Start Search</button>
</form>
<script>
function toggleGroup(name){
  const groups=document.querySelectorAll('.group');
  groups.forEach(div=>{
    const h=div.querySelector('h3');
    if(h && h.textContent.trim()===name){
      const boxes=div.querySelectorAll('input[type="checkbox"]');
      const allChecked=[...boxes].every(b=>b.checked);
      boxes.forEach(b=>b.checked=!allChecked);
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

    if not cats:
        cats = ["Restaurants"]  # fallback so it does something

    import threading
    t = threading.Thread(target=run_scraper_process, args=(cats, zipc, rad))
    t.daemon = True
    t.start()

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
.navbar a{color:#00bfff;margin:0 10px;text-decoration:none}
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
  try{
    const r = await fetch('/logs');
    const d = await r.json();
    const box = document.getElementById('log-box');
    box.innerHTML = d.logs.map(l => '<div>'+l+'</div>').join('');
    box.scrollTop = box.scrollHeight;
  }catch(e){
    // ignore errors silently
  }
}
setInterval(fetchLogs, 2000);
</script>
"""
    return render_template_string(html)

@app.route("/logs")
def logs():
    return jsonify({"logs": scraper_logs})

@app.route("/previous")
def previous():
    if not os.path.exists("runs"):
        files = []
    else:
        files = sorted(os.listdir("runs"))
    links = "".join(
        f"<li><a href='/runs/{f}'>{f}</a></li>" for f in files
    )
    html = (
        BASE_STYLE
        + "<div class='navbar'>"
        "<a href='/'>Home</a> | "
        "<a href='/about'>About</a> | "
        "<a href='/help'>Help</a>"
        "</div>"
        "<h1>Previous Runs</h1>"
        f"<ul>{links}</ul>"
    )
    return render_template_string(html)

@app.route("/runs/<path:filename>")
def download_run(filename):
    return send_from_directory("runs", filename, as_attachment=True)

@app.route("/about")
def about():
    html = (
        BASE_STYLE
        + "<div class='navbar'>"
        "<a href='/'>Home</a> | "
        "<a href='/previous'>Previous Runs</a> | "
        "<a href='/help'>Help</a>"
        "</div>"
        "<h1>About</h1>"
        "<p>Business Lead Scraper (Bertha v1) searches local businesses via Google Places, "
        "scrapes websites for emails and phone numbers, and uploads contacts into Brevo.</p>"
    )
    return render_template_string(html)

@app.route("/help")
def help_page():
    html = (
        BASE_STYLE
        + "<div class='navbar'>"
        "<a href='/'>Home</a> | "
        "<a href='/previous'>Previous Runs</a> | "
        "<a href='/about'>About</a>"
        "</div>"
        "<h1>Help</h1>"
        "<p>1 Select one or more business categories.<br>"
        "2 Enter a ZIP code and search radius.<br>"
        "3 Click Start Search.<br>"
        "4 Watch logs on the run page to see which contacts were found and sent to Brevo.<br>"
        "Contacts with emails go to list 3. Contacts with no email but a phone/website go to list 5.</p>"
    )
    return render_template_string(html)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)