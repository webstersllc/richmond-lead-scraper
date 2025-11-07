import os
import requests
import json
from flask import Flask, render_template_string, request, jsonify
from datetime import datetime
import time
import re
import pandas as pd
import threading

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
BREVO_API_KEY = os.getenv("BREVO_API_KEY")

if not GOOGLE_API_KEY or not BREVO_API_KEY:
    raise ValueError("Missing GOOGLE_API_KEY or BREVO_API_KEY")

app = Flask(__name__)
scraper_logs = []
seen_emails = set()
scraper_in_progress = False

TIMEOUT_SECONDS = 180
MIN_LEADS = 30

def log_message(message):
    timestamp = datetime.now().strftime("%H:%M:%S")
    entry = f"[{timestamp}] {message}"
    print(entry)
    scraper_logs.append(entry)
    if len(scraper_logs) > 400:
        scraper_logs.pop(0)

def get_businesses_from_google(category, zipcode, radius_miles, start_time):
    radius_meters = int(radius_miles) * 1609
    query = f"{category} near {zipcode}"
    url = f"https://maps.googleapis.com/maps/api/place/textsearch/json?query={query}&radius={radius_meters}&key={GOOGLE_API_KEY}"
    log_message(f"🔎 Searching {category} near {zipcode} ({radius_miles} mi radius)…")
    resp = requests.get(url)
    results = resp.json().get("results", [])
    log_message(f"📍 Retrieved {len(results)} {category} results total.")
    data = []
    for r in results:
        if time.time() - start_time >= TIMEOUT_SECONDS:
            log_message("⏱ Timeout while fetching Google data.")
            break
        name = r.get("name", "Unknown Business")
        pid = r.get("place_id")
        deturl = f"https://maps.googleapis.com/maps/api/place/details/json?place_id={pid}&fields=name,website,formatted_phone_number&key={GOOGLE_API_KEY}"
        det = requests.get(deturl).json().get("result", {})
        data.append({
            "name": name,
            "website": det.get("website", ""),
            "phone": det.get("formatted_phone_number", "")
        })
        time.sleep(0.2)
    return data

def find_email_on_website(url):
    if not url:
        return ""
    try:
        r = requests.get(url, timeout=6)
        emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", r.text)
        for e in emails:
            if not any(bad in e for bad in ["example","wixpress","sentry","schema"]):
                return e
    except Exception:
        pass
    return ""

def find_owner_name_and_phone(url):
    if not url:
        return "", ""
    try:
        r = requests.get(url, timeout=6)
        txt = re.sub(r"<[^>]*>", " ", r.text)
        txt = re.sub(r"\s+", " ", txt)
        for line in txt.split("."):
            if any(k in line.lower() for k in ["owner","ceo","founder","manager","director","president"]):
                nm = re.search(r"\b([A-Z][a-z]+ [A-Z][a-z]+)\b", line)
                if nm:
                    ph = re.search(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", txt)
                    return nm.group(1), ph.group(0) if ph else ""
        ph = re.search(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", txt)
        return "", ph.group(0) if ph else ""
    except Exception:
        return "", ""

def add_to_brevo(contact, has_email=True):
    url = "https://api.brevo.com/v3/contacts"
    headers = {"accept":"application/json","content-type":"application/json","api-key":BREVO_API_KEY}
    payload = {
        "email": contact.get("email") if has_email else f"{contact['name'].replace(' ','').lower()}@placeholder.com",
        "attributes": {
            "FIRSTNAME": contact.get("owner_name") or contact.get("name"),
            "COMPANY": contact.get("name"),
            "PHONE": contact.get("phone",""),
            "WEBSITE": contact.get("website","")
        },
        "listIds": [3 if has_email else 5]
    }
    r = requests.post(url, headers=headers, data=json.dumps(payload))
    log_message(f"Added to Brevo (List {'3' if has_email else '5'}): {contact.get('email','no email')} ({r.status_code})")

def run_scraper_process(categories, zipcode, radius):
    global scraper_in_progress
    if scraper_in_progress:
        log_message("⚠️ A scraper is already running. Please wait for it to finish.")
        return
    scraper_in_progress = True

    scraper_logs.clear()
    seen_emails.clear()
    log_message("🚀 Scraper started.")
    start_time = time.time()
    results, uploaded = [], 0
    next_log = 60

    for c in categories:
        if time.time() - start_time >= TIMEOUT_SECONDS:
            log_message("⏱ Timeout reached while searching.")
            break
        results.extend(get_businesses_from_google(c, zipcode, radius, start_time))

    for biz in results[:400]:
        elapsed = time.time() - start_time
        if elapsed >= TIMEOUT_SECONDS:
            log_message("⏱ Timeout reached — finalizing uploads.")
            break
        if uploaded >= MIN_LEADS:
            log_message("✅ Minimum 30 leads reached — stopping early.")
            break

        email = find_email_on_website(biz["website"])
        owner, phone = find_owner_name_and_phone(biz["website"])
        contact = {"name": biz["name"], "phone": phone or biz["phone"], "website": biz["website"],
                   "email": email, "owner_name": owner}

        if email and email in seen_emails:
            log_message(f"⚠️ Duplicate skipped: {email}")
            continue

        if email:
            add_to_brevo(contact, True)
            seen_emails.add(email)
            uploaded += 1
            log_message(f"✅ {biz['name']} ({email}) → List 3")
        else:
            add_to_brevo(contact, False)
            uploaded += 1
            log_message(f"📇 {biz['name']} (No Email) → List 5")

        if elapsed >= next_log and elapsed < TIMEOUT_SECONDS:
            remaining = TIMEOUT_SECONDS - int(elapsed)
            mins = remaining // 60
            secs = remaining % 60
            log_message(f"⏱ {mins}:{secs:02d} remaining...")
            next_log += 60

        time.sleep(0.5)

    os.makedirs("runs", exist_ok=True)
    fname = f"runs/run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    pd.DataFrame(results).to_excel(fname, index=False)
    log_message(f"📁 Saved as {fname}")
    log_message(f"🎯 Finished — {uploaded} uploaded.")
    scraper_in_progress = False

BASE_STYLE = """
<style>
body{background:#000;color:#00bfff;font-family:Consolas,monospace;text-align:center;padding:20px}
h1{color:#00bfff}h2{color:#0099ff}
button,input[type=text]{padding:10px;margin:5px;border-radius:6px;font-weight:bold}
.navbar{margin-bottom:20px}
.navbar a{color:#00bfff;margin:0 10px;text-decoration:none}
#log-box{width:80%;margin:20px auto;text-align:left;height:400px;overflow-y:auto;background:#0a0a0a;
border:1px solid #00bfff;border-radius:10px;padding:20px}
#timer{font-size:18px;margin-top:10px;color:#00ffcc}
.grid{display:flex;flex-wrap:wrap;justify-content:center;gap:15px;margin-top:10px}
.group{border:1px solid #00bfff;border-radius:10px;padding:10px;width:250px}
.group h3{color:#00bfff;cursor:pointer;text-decoration:underline}

@media (max-width:600px){
  body{padding:10px}
  button,input[type=text]{width:90%;font-size:16px;margin:8px auto;display:block}
  #log-box{width:95%;height:300px}
  .group{width:90%}
  h1{font-size:1.8em}h2{font-size:1.1em}
}
</style>
"""

@app.route("/")
def home():
    grouped = {
        "Food & Drink":["Restaurants","Bars & Clubs","Coffee Shops","Bakeries","Breweries","Cafes","Juice Bars"],
        "Retail & Shopping":["Retail Stores","Boutiques","Clothing Stores","Gift Shops","Bookstores","Home Goods Stores"],
        "Beauty & Wellness":["Salons","Barbers","Spas","Massage Therapy","Nail Salons"],
        "Fitness & Recreation":["Gyms","Yoga Studios","Martial Arts","CrossFit","Dance Studios"],
        "Home Services":["HVAC","Plumbing","Electricians","Landscaping","Cleaning Services","Painting","Roofing","Pest Control"],
        "Auto Services":["Auto Repair","Car Wash","Tire Shops","Car Dealerships","Detailing"],
        "Insurance & Finance":["Insurance Agencies","Banks","Credit Unions","Financial Advisors"],
        "Events & Entertainment":["Event Venues","Wedding Planners","Catering","Escape Rooms","Putt Putt","Bowling Alleys"],
        "Construction & Real Estate":["Construction Companies","Contractors","Real Estate Agencies","Home Builders"],
        "Health & Medical":["Dentists","Doctors","Chiropractors","Physical Therapy","Veterinarians"],
        "Pets":["Pet Groomers","Pet Boarding","Pet Stores"],
        "Education & Childcare":["Daycares","Private Schools","Tutoring Centers","Learning Centers"],
        "Professional Services":["Law Firms","Accountants","Consulting Firms"],
        "Community & Nonprofits":["Churches","Nonprofits","Community Centers"]
    }

    html = f"""{BASE_STYLE}
<div class='navbar'>
 <a href='/'>Home</a> | <a href='/previous'>Previous Runs</a> | <a href='/about'>About</a> | <a href='/help'>Help</a>
</div>
<h1>Business Lead Scraper</h1>
<h2>Select categories and enter ZIP & radius</h2>
<form action='/run' method='get'><div class='grid'>"""
    for g,cats in grouped.items():
        html += f"<div class='group'><h3 onclick=\"toggleGroup('{g}')\">{g}</h3>"
        for c in cats:
            html += f"<label><input type='checkbox' name='categories' value='{c}'> {c}</label><br>"
        html += "</div>"
    html += """</div><br>
ZIP Code: <input type='text' name='zipcode' required>
Radius (mi): <input type='text' name='radius' required value='10'><br><br>
<button type='submit'>Start Search</button></form>
<script>
function toggleGroup(name){
  const groups=document.querySelectorAll('.group');
  groups.forEach(div=>{
    const h=div.querySelector('h3');
    if(h&&h.textContent.trim()===name){
      const boxes=div.querySelectorAll('input[type="checkbox"]');
      const allChecked=[...boxes].every(b=>b.checked);
      boxes.forEach(b=>b.checked=!allChecked);
    }
  });
}
</script>"""
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
    body{background:#000;color:#00bfff;font-family:Consolas,monospace;text-align:center;padding:20px}
    #log-box{width:80%;margin:20px auto;text-align:left;height:400px;overflow-y:auto;background:#0a0a0a;
    border:1px solid #00bfff;border-radius:10px;padding:20px}
    #timer{font-size:20px;margin-top:10px;color:#00ffcc}
    @media (max-width:600px){#log-box{width:95%;height:300px;font-size:14px}#timer{font-size:18px}}
    </style>

    <h1>Business Lead Scraper</h1>
    <h2>Running… Logs below</h2>
    <div id="timer">⏱ 3:00 remaining</div>
    <div id="log-box"></div>

    {% raw %}
    <script>
    let remaining = 180;
    function updateTimer(){
      if(remaining <= 0){
        document.getElementById('timer').innerText = '✅ Finished';
        return;
      }
      let mins = Math.floor(remaining/60);
      let secs = remaining % 60;
      document.getElementById('timer').innerText = '⏱ ' + mins + ':' + secs.toString().padStart(2,'0') + ' remaining';
      remaining--;
    }
    setInterval(updateTimer,1000);

    async function fetchLogs(){
      const r = await fetch('/logs');
      const d = await r.json();
      const box = document.getElementById('log-box');
      box.innerHTML = d.logs.map(l => '<div>' + l + '</div>').join('');
      box.scrollTop = box.scrollHeight;
    }
    setInterval(fetchLogs,2000);
    </script>
    {% endraw %}
    """

    return render_template_string(html)

@app.route("/previous")
def previous():
    files=os.listdir("runs") if os.path.exists("runs") else []
    links="".join(f"<li><a href='/runs/{f}'>{f}</a></li>" for f in files)
    return render_template_string(f"{BASE_STYLE}<div class='navbar'><a href='/'>Home</a></div><h1>Previous Runs</h1><ul>{links}</ul>")

@app.route("/about")
def about():
    return render_template_string(f"{BASE_STYLE}<div class='navbar'><a href='/'>Home</a></div><h1>About</h1><p>Business Lead Scraper locates local businesses via Google Places, extracts contact info, and uploads results to Brevo lists.</p>")

@app.route("/help")
def help_page():
    return render_template_string(f"{BASE_STYLE}<div class='navbar'><a href='/'>Home</a></div><h1>Help</h1><p>1 Select categories.<br>2 Enter ZIP and radius.<br>3 Click Start Search.<br>4 View logs live as data collects.</p>")

@app.route("/logs")
def logs():
    return jsonify({"logs": scraper_logs})

if __name__=="__main__":
    app.run(host="0.0.0.0",port=10000)
