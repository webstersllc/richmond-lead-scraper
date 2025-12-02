import os
import requests
import json
from flask import Flask, render_template_string, request, jsonify
from datetime import datetime
import time
import re
import pandas as pd

# --------------------------------------------------------------------
# Environment
# --------------------------------------------------------------------
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
BREVO_API_KEY = os.getenv("BREVO_API_KEY")

if not GOOGLE_API_KEY or not BREVO_API_KEY:
    raise ValueError("Missing GOOGLE_API_KEY or BREVO_API_KEY")

app = Flask(__name__)

scraper_logs = []
seen_emails = set()
scraper_in_progress = False  # prevent multiple runs in parallel

# Emails to avoid (example / dummy emails)
AVOID_EMAILS = {
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

# extra patterns we never want
BAD_EMAIL_SUBSTRINGS = [
    "example.com",
    "wixpress",
    "sentry",
    "schema.org",
    "domain.com",
    "website.com",
    "mysite.com",
    "email.com",
    "sample.com",
    "demo.com",
]


# --------------------------------------------------------------------
# Logging helper
# --------------------------------------------------------------------
def log_message(message: str) -> None:
    timestamp = datetime.now().strftime("%H:%M:%S")
    entry = f"[{timestamp}] {message}"
    print(entry)
    scraper_logs.append(entry)
    if len(scraper_logs) > 400:
        scraper_logs.pop(0)


# --------------------------------------------------------------------
# Google Places helper
# --------------------------------------------------------------------
def get_businesses_from_google(category: str, zipcode: str, radius_miles: str, max_results: int = 60):
    radius_meters = int(radius_miles) * 1609
    query = f"{category} near {zipcode}"
    url = (
        "https://maps.googleapis.com/maps/api/place/textsearch/json"
        f"?query={requests.utils.quote(query)}&radius={radius_meters}&key={GOOGLE_API_KEY}"
    )
    log_message(f"🔎 Searching {category} near {zipcode} ({radius_miles} mi radius)…")

    all_results = []
    page_token = None

    while True:
        final_url = url
        if page_token:
            final_url += f"&pagetoken={page_token}"

        resp = requests.get(final_url)
        data = resp.json()
        results = data.get("results", [])
        all_results.extend(results)

        if len(all_results) >= max_results:
            break

        page_token = data.get("next_page_token")
        if not page_token:
            break

        time.sleep(2.0)

    log_message(f"📍 Retrieved {len(all_results)} {category} results total.")

    businesses = []
    for r in all_results[:max_results]:
        name = r.get("name", "Unknown Business")
        pid = r.get("place_id")
        details_url = (
            "https://maps.googleapis.com/maps/api/place/details/json"
            f"?place_id={pid}&fields=name,website,formatted_phone_number&key={GOOGLE_API_KEY}"
        )
        det = requests.get(details_url).json().get("result", {})
        businesses.append(
            {
                "name": name,
                "website": det.get("website", ""),
                "phone": det.get("formatted_phone_number", ""),
                "category": category,
            }
        )
        time.sleep(0.2)

    return businesses


# --------------------------------------------------------------------
# Phone helpers
# --------------------------------------------------------------------
def normalize_phone_for_sms(raw_phone: str) -> str:
    """
    Convert things like '804-555-1234' or '(804) 555 1234'
    into +18045551234 so Brevo will accept it in the SMS field.
    """
    if not raw_phone:
        return ""

    digits = re.sub(r"\D", "", raw_phone)

    # US 10-digit number → +1XXXXXXXXXX
    if len(digits) == 10:
        return "+1" + digits

    # 11-digit starting with 1 → +1XXXXXXXXXX
    if len(digits) == 11 and digits.startswith("1"):
        return "+" + digits

    # Fallback: return original
    return raw_phone


# --------------------------------------------------------------------
# Email + owner extraction from website
# --------------------------------------------------------------------
def find_email_on_website(url: str) -> str:
    if not url:
        return ""
    try:
        r = requests.get(url, timeout=6)
        emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", r.text)
        for e in emails:
            e_lower = e.lower()
            if e_lower in AVOID_EMAILS:
                continue
            if any(bad in e_lower for bad in BAD_EMAIL_SUBSTRINGS):
                continue
            return e
    except Exception as exc:
        log_message(f"Error scanning {url} for email: {exc}")
    return ""


def find_owner_name_and_phone(url: str):
    if not url:
        return "", ""
    try:
        r = requests.get(url, timeout=6)
        txt = re.sub(r"<[^>]*>", " ", r.text)
        txt = re.sub(r"\s+", " ", txt)

        owner_keywords = ["owner", "ceo", "founder", "manager", "director", "president"]

        for line in txt.split("."):
            if any(k in line.lower() for k in owner_keywords):
                nm = re.search(r"\b([A-Z][a-z]+ [A-Z][a-z]+)\b", line)
                if nm:
                    ph_match = re.search(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", txt)
                    phone = ph_match.group(0) if ph_match else ""
                    return nm.group(1), phone

        ph_match = re.search(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", txt)
        phone = ph_match.group(0) if ph_match else ""
        return "", phone
    except Exception as exc:
        log_message(f"Error parsing {url} for owner/phone: {exc}")
        return "", ""


# --------------------------------------------------------------------
# Brevo insertion
# --------------------------------------------------------------------
def add_to_brevo(contact: dict, has_email: bool = True):
    """
    Send contact to Brevo:
      - List 3 if it has an email
      - List 5 if it does not (we generate a placeholder email)
      - Map phone to BOTH:
          - attributes['PHONE'] (for your custom column if you enable it)
          - attributes['sms']   (what Brevo actually uses for phone/SMS)
    """

    url = "https://api.brevo.com/v3/contacts"
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "api-key": BREVO_API_KEY,
    }

    raw_phone = (contact.get("phone") or "").strip()
    sms_phone = normalize_phone_for_sms(raw_phone)

    # Make sure first name is something useful
    firstname = contact.get("owner_name") or contact.get("name") or ""

    attrs = {
        "FIRSTNAME": firstname,
        "COMPANY": contact.get("name", ""),
        "PHONE": raw_phone,
        "WEBSITE": contact.get("website", ""),
    }
    if sms_phone:
        # This is the key Brevo uses for phone
        attrs["sms"] = sms_phone

    email_value = contact.get("email") if has_email else f"{contact['name'].replace(' ', '').lower()}@placeholder.com"

    payload = {
        "email": email_value,
        "attributes": attrs,
        "listIds": [3 if has_email else 5],
    }

    r = requests.post(url, headers=headers, data=json.dumps(payload))

    log_message(
        f"Added to Brevo (List {'3' if has_email else '5'}): "
        f"{email_value} | phone_raw='{raw_phone}' sms='{sms_phone}' ({r.status_code})"
    )


# --------------------------------------------------------------------
# Core scraper process
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

    TIMEOUT_SECONDS = 180  # 3 minutes
    MIN_CONTACTS = 30
    MAX_BUSINESSES = 400

    start_time = time.time()
    all_businesses = []
    seen_business_keys = set()

    # 1. Gather businesses from all selected categories
    for c in categories:
        if time.time() - start_time > TIMEOUT_SECONDS and len(all_businesses) >= MIN_CONTACTS:
            log_message("⏱ Timeout reached while fetching businesses; continuing with what we have.")
            break

        biz_list = get_businesses_from_google(c, zipcode, radius)
        for b in biz_list:
            key = (b["name"], b["website"])
            if key not in seen_business_keys:
                seen_business_keys.add(key)
                all_businesses.append(b)

        if len(all_businesses) >= MAX_BUSINESSES:
            log_message(f"⛔ Hit MAX_BUSINESSES limit of {MAX_BUSINESSES}.")
            break

    log_message(f"📊 Total unique businesses collected: {len(all_businesses)}")

    # 2. Process each business, upload to Brevo, and store for Excel
    uploaded = 0
    rows_for_excel = []

    for biz in all_businesses:
        if time.time() - start_time > TIMEOUT_SECONDS and uploaded >= MIN_CONTACTS:
            log_message("⏱ Timeout reached during processing; stopping uploads.")
            break

        website = biz.get("website", "")
        base_phone = biz.get("phone", "")

        email = find_email_on_website(website)
        owner, phone_from_site = find_owner_name_and_phone(website)

        final_phone = phone_from_site or base_phone

        contact = {
            "name": biz["name"],
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
            rows_for_excel.append(
                {
                    "Business Name": biz["name"],
                    "Email": email,
                    "Phone": final_phone,
                    "Website": website,
                    "Owner Name": owner,
                    "Category": biz.get("category", ""),
                    "List": "3",
                }
            )
        else:
            add_to_brevo(contact, has_email=False)
            uploaded += 1
            log_message(f"📇 {biz['name']} (No Email) → List 5")
            rows_for_excel.append(
                {
                    "Business Name": biz["name"],
                    "Email": "",
                    "Phone": final_phone,
                    "Website": website,
                    "Owner Name": owner,
                    "Category": biz.get("category", ""),
                    "List": "5",
                }
            )

        time.sleep(0.5)

    # 3. Save to Excel
    try:
        os.makedirs("runs", exist_ok=True)
        fname = f"runs/run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        df = pd.DataFrame(rows_for_excel)
        df.to_excel(fname, index=False)
        log_message(f"📁 Saved as {fname}")
    except Exception as exc:
        log_message(f"⚠️ Failed to save Excel: {exc}")

    log_message(f"🎯 Finished — {uploaded} uploaded.")
    scraper_in_progress = False


# --------------------------------------------------------------------
# Basic shared styles for all pages
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
h1{color:#00bfff;}
h2{color:#0099ff;}
button,input[type=text]{
  padding:10px;
  margin:5px;
  border-radius:6px;
  font-weight:bold;
}
.navbar{margin-bottom:20px;}
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
  width:260px;
}
.group h3{
  color:#00bfff;
  cursor:pointer;
  text-decoration:underline;
}
@media(max-width:768px){
  #log-box{width:95%;}
  .group{width:90%;}
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
            "Restaurants",
            "Bars & Clubs",
            "Coffee Shops",
            "Bakeries",
            "Breweries",
            "Cafes",
            "Juice Bars",
        ],
        "Retail & Shopping": [
            "Retail Stores",
            "Boutiques",
            "Clothing Stores",
            "Gift Shops",
            "Bookstores",
            "Home Goods Stores",
        ],
        "Beauty & Wellness": [
            "Salons",
            "Barbers",
            "Spas",
            "Massage Therapy",
            "Nail Salons",
        ],
        "Fitness & Recreation": [
            "Gyms",
            "Yoga Studios",
            "Martial Arts",
            "CrossFit",
            "Dance Studios",
        ],
        "Home Services": [
            "HVAC",
            "Plumbing",
            "Electricians",
            "Landscaping",
            "Cleaning Services",
            "Painting",
            "Roofing",
            "Pest Control",
        ],
        "Auto Services": [
            "Auto Repair",
            "Car Wash",
            "Tire Shops",
            "Car Dealerships",
            "Detailing",
        ],
        "Insurance & Finance": [
            "Insurance Agencies",
            "Banks",
            "Credit Unions",
            "Financial Advisors",
        ],
        "Events & Entertainment": [
            "Event Venues",
            "Wedding Planners",
            "Catering",
            "Escape Rooms",
            "Putt Putt",
            "Bowling Alleys",
        ],
        "Construction & Real Estate": [
            "Construction Companies",
            "Contractors",
            "Real Estate Agencies",
            "Home Builders",
        ],
        "Health & Medical": [
            "Dentists",
            "Doctors",
            "Chiropractors",
            "Physical Therapy",
            "Veterinarians",
        ],
        "Pets": [
            "Pet Groomers",
            "Pet Boarding",
            "Pet Stores",
        ],
        "Education & Childcare": [
            "Daycares",
            "Private Schools",
            "Tutoring Centers",
            "Learning Centers",
        ],
        "Professional Services": [
            "Law Firms",
            "Accountants",
            "Consulting Firms",
        ],
        "Community & Nonprofits": [
            "Churches",
            "Nonprofits",
            "Community Centers",
        ],
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

    for group_name, cats in grouped.items():
        html += f"<div class='group'><h3 onclick=\"toggleGroup('{group_name}')\">{group_name}</h3>"
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
h1{color:#00bfff;}
h2{color:#0099ff;}
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
@media(max-width:768px){
  #log-box{width:95%;}
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
<p>Business Lead Scraper uses Google Places to find local businesses, extracts emails and phone numbers from their websites, and uploads them into Brevo:
<br>List 3 = contacts with email
<br>List 5 = contacts with no email but usable phone</p>
"""
    )


@app.route("/help")
def help_page():
    return render_template_string(
        f"""{BASE_STYLE}
<div class='navbar'><a href='/'>Home</a></div>
<h1>Help</h1>
<p>
1. Select one or more categories.<br>
2. Enter the ZIP code and radius in miles.<br>
3. Click "Start Search".<br>
4. Watch the logs as businesses are discovered and uploaded to Brevo.<br>
Timeout is 3 minutes; if at least 30 contacts are uploaded, it will stop early when the timer hits.
</p>
"""
    )


@app.route("/logs")
def logs():
    return jsonify({"logs": scraper_logs})


# static file serving for /runs/*.xlsx if you want to hook that up later
@app.route("/runs/<path:filename>")
def download_run(filename):
    from flask import send_from_directory

    return send_from_directory("runs", filename, as_attachment=True)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)