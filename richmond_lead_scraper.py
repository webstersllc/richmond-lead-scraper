import os
import requests
import json
import re
import pandas as pd
import zipfile
from datetime import datetime
from io import BytesIO
from flask import Flask, render_template_string, jsonify, request, send_file
import threading
from urllib.parse import urlencode

# ============================================================
#  CONFIG
# ============================================================
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
BREVO_API_KEY = os.getenv("BREVO_API_KEY")
if not GOOGLE_API_KEY or not BREVO_API_KEY:
    raise ValueError("Missing environment variables for GOOGLE_API_KEY or BREVO_API_KEY")

SCRAPE_FOLDER = "scrapes"
os.makedirs(SCRAPE_FOLDER, exist_ok=True)

# ============================================================
#  APP INITIALIZATION
# ============================================================
app = Flask(__name__)
scraper_logs = []
seen_emails = set()

# ============================================================
#  LOGGING
# ============================================================
def log(msg):
    timestamp = datetime.now().strftime("%H:%M:%S")
    entry = f"[{timestamp}] {msg}"
    print(entry)
    scraper_logs.append(entry)
    if len(scraper_logs) > 400:
        scraper_logs.pop(0)

# ============================================================
#  BREVO UPLOAD
# ============================================================
def add_to_brevo(contact, list_id):
    url = "https://api.brevo.com/v3/contacts"
    headers = {"accept": "application/json", "content-type": "application/json", "api-key": BREVO_API_KEY}
    data = {
        "email": contact.get("email", ""),
        "attributes": {
            "FIRSTNAME": contact.get("owner_name", contact.get("name", "")),
            "COMPANY": contact.get("name", ""),
            "PHONE": contact.get("phone", ""),
            "WEBSITE": contact.get("website", "")
        },
        "listIds": [list_id]
    }
    r = requests.post(url, headers=headers, data=json.dumps(data))
    log(f"📤 Uploaded {contact.get('email','(no email)')} to Brevo list {list_id} ({r.status_code})")

# ============================================================
#  HELPERS
# ============================================================
def find_owner_name_and_phone(website):
    if not website:
        return "", ""
    try:
        resp = requests.get(website, timeout=6)
        text = re.sub(r"<[^>]*>", " ", resp.text)
        text = re.sub(r"\s+", " ", text)

        owner_keywords = ["owner", "founder", "ceo", "manager", "president", "director"]
        owner_name = ""
        for line in text.split("."):
            if any(k in line.lower() for k in owner_keywords):
                m = re.search(r"\b([A-Z][a-z]+ [A-Z][a-z]+)\b", line)
                if m:
                    owner_name = m.group(1)
                    break
        phone_match = re.search(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", text)
        phone = phone_match.group(0) if phone_match else ""
        return owner_name, phone
    except Exception:
        return "", ""

def find_email_on_website(website):
    if not website:
        return ""
    try:
        resp = requests.get(website, timeout=6)
        emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", resp.text)
        valid = [e for e in emails if not any(x in e for x in ["example.com","schema.org","wixpress"])]
        return valid[0] if valid else ""
    except Exception:
        return ""

def get_businesses(category, zipcode, radius_miles):
    log(f"📍 Searching {category} near {zipcode} ({radius_miles} miles)")
    radius_meters = int(radius_miles * 1609.34)
    query = f"{category} near {zipcode}"
    url = f"https://maps.googleapis.com/maps/api/place/textsearch/json?{urlencode({'query':query,'radius':radius_meters,'key':GOOGLE_API_KEY})}"
    resp = requests.get(url).json()
    results = resp.get("results", [])
    businesses = []
    for b in results:
        name = b.get("name")
        place_id = b.get("place_id")
        details_url = f"https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&fields=name,website,formatted_phone_number&key={GOOGLE_API_KEY}"
        det = requests.get(details_url).json().get("result", {})
        businesses.append({
            "name": name,
            "website": det.get("website", ""),
            "phone": det.get("formatted_phone_number", "")
        })
    log(f"✅ Found {len(businesses)} {category} listings.")
    return businesses

# ============================================================
#  MAIN SCRAPER PROCESS
# ============================================================
def run_scraper(categories, zipcode, radius):
    scraper_logs.clear()
    seen_emails.clear()
    log("🚀 Starting scraper...")
    leads = []
    for cat in categories:
        for biz in get_businesses(cat, zipcode, radius):
            name = biz.get("name", "Unknown Business")
            website = biz.get("website", "")
            phone = biz.get("phone", "")
            email = find_email_on_website(website)
            owner, found_phone = find_owner_name_and_phone(website)
            phone = phone or found_phone

            contact = {
                "name": name,
                "email": email,
                "phone": phone,
                "website": website,
                "owner_name": owner or name,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }

            if email and email not in seen_emails:
                add_to_brevo(contact, 3)
                uploaded_to = "Brevo List 3 (email)"
                seen_emails.add(email)
            elif not email and phone:
                add_to_brevo(contact, 5)
                uploaded_to = "Brevo List 5 (phone)"
            else:
                uploaded_to = "Skipped (no contact info)"
            contact["uploaded_to"] = uploaded_to
            leads.append(contact)
    save_scrape(leads)
    log(f"🎯 Scraper finished — {len(leads)} total leads collected.")

# ============================================================
#  SAVE SCRAPES
# ============================================================
def save_scrape(leads):
    if not leads: return
    df = pd.DataFrame(leads)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    base = os.path.join(SCRAPE_FOLDER, f"scrape_{timestamp}")
    xlsx = base + ".xlsx"
    jsonf = base + ".json"
    df.to_excel(xlsx, index=False)
    df.to_json(jsonf, orient="records", indent=2)
    zipf = base + ".zip"
    with zipfile.ZipFile(zipf, "w") as z:
        z.write(xlsx, os.path.basename(xlsx))
        z.write(jsonf, os.path.basename(jsonf))
    os.remove(xlsx)
    os.remove(jsonf)
    log(f"💾 Saved results to {os.path.basename(zipf)}")

# ============================================================
#  ROUTES
# ============================================================
@app.route("/")
def index():
    categories = ["Restaurants", "HVAC Companies", "Landscaping", "Coffee Shops",
                  "Gyms", "Real Estate", "Boutiques", "Auto Repair", "Salons", "Contractors"]
    return render_template_string("""
<!DOCTYPE html><html><head><title>Richmond Lead Scraper</title>
<style>
body{background:#000;color:#00bfff;font-family:Consolas;text-align:center;padding:40px;}
h1{color:#00bfff;margin-bottom:10px;}label{font-size:1.2em;}
input,select{padding:8px;border-radius:6px;border:none;margin:5px;}
button{background:#00bfff;color:#000;padding:12px 24px;border:none;
border-radius:8px;font-weight:bold;cursor:pointer;margin-top:10px;}
button:hover{background:#0088cc;}
.category-btn{margin:5px;padding:10px 20px;border:1px solid #00bfff;
border-radius:6px;cursor:pointer;color:#00bfff;background:none;}
.category-btn.active{background:#00bfff;color:#000;}
</style></head><body>
<h1>Richmond Lead Scraper</h1>
<label>ZIP Code:</label><input id="zip" placeholder="e.g. 23005">
<label>Radius (miles):</label><input id="radius" type="number" value="10">
<div id="categories">
{% for c in categories %}
  <button class="category-btn" onclick="toggleCat(this)">{{c}}</button>
{% endfor %}
</div>
<br><button onclick="startScraper()">Start Scraper</button>
<script>
let selected=[]
function toggleCat(btn){
  let cat=btn.innerText
  if(selected.includes(cat)){selected=selected.filter(c=>c!=cat);btn.classList.remove("active")}
  else{selected.push(cat);btn.classList.add("active")}
}
function startScraper(){
  let zip=document.getElementById('zip').value
  let r=document.getElementById('radius').value
  if(!zip){alert('Enter ZIP');return}
  fetch(`/run?zip=${zip}&radius=${r}&categories=${encodeURIComponent(selected.join(','))}`)
  .then(()=>window.location='/run')
}
</script>
</body></html>""", categories=categories)

@app.route("/run")
def run_page():
    return render_template_string("""
<!DOCTYPE html><html><head><title>Scraper Running</title>
<style>
body{background:#000;color:#00bfff;font-family:Consolas;text-align:center;padding:30px;}
#log{background:#0a0a0a;border:1px solid #00bfff;padding:20px;width:90%;
margin:auto;border-radius:10px;height:400px;overflow-y:auto;}
button{background:#00bfff;color:#000;padding:10px 20px;border:none;border-radius:6px;
font-weight:bold;cursor:pointer;margin:10px;}
</style></head><body>
<h1>Scraper Logs</h1>
<button onclick="window.location='/'">Back</button>
<div id="log"></div>
<script>
async function refresh(){let r=await fetch('/logs');let d=await r.json();
let box=document.getElementById('log');box.innerHTML=d.logs.map(l=>'<div>'+l+'</div>').join('');
box.scrollTop=box.scrollHeight}
setInterval(refresh,2000)
</script></body></html>""")

@app.route("/scrapes")
def scrapes_page():
    files=[f for f in os.listdir(SCRAPE_FOLDER) if f.endswith('.zip')]
    files.sort(reverse=True)
    links="".join(f"<div><a href='/download/{f}'>{f}</a> "
                  f"<a href='/delete/{f}' style='color:red'>❌</a></div>" for f in files) or "No saved scrapes yet."
    return f"<body style='background:#000;color:#00bfff;font-family:Consolas;padding:30px;'>" \
           f"<h1>Saved Scrapes</h1>{links}<br><a href='/' style='color:#00bfff;'>Back</a></body>"

@app.route("/download/<path:fname>")
def download(fname):
    return send_file(os.path.join(SCRAPE_FOLDER, fname), as_attachment=True)

@app.route("/delete/<path:fname>")
def delete(fname):
    os.remove(os.path.join(SCRAPE_FOLDER, fname))
    log(f"🗑 Deleted {fname}")
    return "<meta http-equiv='refresh' content='0; url=/scrapes'/>"

@app.route("/logs")
def logs():
    return jsonify({"logs": scraper_logs})

@app.route("/run", methods=["GET"])
def start_scraper():
    zipc = request.args.get("zip", "23005")
    radius = float(request.args.get("radius", 10))
    categories = request.args.get("categories", "Restaurants").split(",")
    threading.Thread(target=run_scraper, args=(categories, zipc, radius)).start()
    return jsonify({"status": "started"})

# ============================================================
#  MAIN
# ============================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)

