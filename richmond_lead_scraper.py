import os, re, json, time, threading
from datetime import datetime
from urllib.parse import urljoin
from pathlib import Path
import pandas as pd
import requests
from flask import Flask, render_template_string, jsonify, send_file, request

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
BREVO_API_KEY  = os.getenv("BREVO_API_KEY")
if not GOOGLE_API_KEY or not BREVO_API_KEY:
    raise ValueError("Missing environment variables for GOOGLE_API_KEY or BREVO_API_KEY")

app = Flask(__name__)
scraper_logs, seen_emails = [], set()
last_export = {"ready": False, "path": "", "name": ""}

def log(msg):
    t = datetime.now().strftime("%H:%M:%S")
    line = f"[{t}] {msg}"
    print(line)
    scraper_logs.append(line)
    if len(scraper_logs) > 400:
        scraper_logs.pop(0)

# ---------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------
def find_owner_name_and_phone(website):
    if not website:
        return "", ""
    try:
        r = requests.get(website, timeout=6)
        html = r.text
        text = re.sub(r"<[^>]+>", " ", html)
        text = re.sub(r"\s+", " ", text)
        about = next((urljoin(website, l) for l in re.findall(r'href=["\'](.*?)["\']', html) if "about" in l.lower()), None)
        if about:
            try:
                r2 = requests.get(about, timeout=6)
                text += " " + re.sub(r"<[^>]+>", " ", r2.text)
            except:
                pass
        owner = ""
        for line in text.split("."):
            if any(k in line.lower() for k in ["owner","ceo","founder","manager","president","director"]):
                m = re.search(r"\b([A-Z][a-z]+ [A-Z][a-z]+)\b", line)
                if m:
                    owner = m.group(1); break
        phone = re.search(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", text)
        return owner, phone.group(0) if phone else ""
    except Exception as e:
        log(f"Parse error {website}: {e}")
        return "",""

def find_email_on_site(url):
    if not url: return ""
    try:
        r = requests.get(url, timeout=6)
        for e in re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", r.text):
            if not any(b in e for b in ["example.","wixpress","schema.org","sentry"]):
                return e
    except Exception as e:
        log(f"Scan error {url}: {e}")
    return ""

def add_to_brevo(c, lid=3):
    if not c.get("email"): return
    headers = {"accept":"application/json","content-type":"application/json","api-key":BREVO_API_KEY}
    payload = {
        "email": c["email"],
        "attributes": {
            "FIRSTNAME": c.get("owner_name",""),
            "COMPANY": c.get("name",""),
            "PHONE": c.get("phone",""),
            "WEBSITE": c.get("website","")
        },
        "listIds":[lid]
    }
    r = requests.post("https://api.brevo.com/v3/contacts", headers=headers, data=json.dumps(payload))
    log(f"Added {c['email']} to Brevo ({r.status_code})")

# ---------------------------------------------------------------
# Google search
# ---------------------------------------------------------------
def get_businesses_from_google(location="Richmond,VA", radius_m=8000, limit=60):
    log(f"Searching {location} ...")
    url = f"https://maps.googleapis.com/maps/api/place/textsearch/json?query=businesses+in+{location}&radius={radius_m}&key={GOOGLE_API_KEY}"
    r = requests.get(url).json()
    results = r.get("results", [])
    out=[]
    for x in results[:limit]:
        pid = x.get("place_id")
        det = requests.get(
            f"https://maps.googleapis.com/maps/api/place/details/json?place_id={pid}&fields=name,website,formatted_phone_number&key={GOOGLE_API_KEY}"
        ).json().get("result",{})
        out.append({
            "name": x.get("name",""),
            "website": det.get("website",""),
            "phone": det.get("formatted_phone_number","")
        })
    log(f"Found {len(out)}")
    return out

# ---------------------------------------------------------------
# Main scrape
# ---------------------------------------------------------------
def run_scraper(location="Richmond,VA"):
    scraper_logs.clear(); seen_emails.clear()
    log("🚀 Starting lead scraper...")
    bizs = get_businesses_from_google(location)
    uploaded=0; data=[]
    for b in bizs:
        email=find_email_on_site(b.get("website"))
        owner,phone=find_owner_name_and_phone(b.get("website"))
        if not owner: owner=b["name"]
        if not phone: phone=b.get("phone","")
        c={"name":b["name"],"phone":phone,"website":b["website"],"email":email,"owner_name":owner}
        if email and email not in seen_emails:
            add_to_brevo(c,3); seen_emails.add(email)
            uploaded+=1; log(f"✅ {b['name']} ({email}) owner: {owner}")
        elif not email and phone:
            add_to_brevo(c,5); log(f"📞 {b['name']} added (no email)")
        elif email in seen_emails:
            log(f"⚠️ Duplicate {email}")
        else:
            log(f"❌ No contact for {b['name']}")
        time.sleep(1.2)
        data.append(c)
    Path("data/exports").mkdir(parents=True, exist_ok=True)
    fn=f"leads_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    fp=f"data/exports/{fn}"
    pd.DataFrame(data).to_excel(fp,index=False)
    last_export.update({"ready":True,"path":fp,"name":fn})
    log(f"🎯 Finished {uploaded} uploads.")

# ---------------------------------------------------------------
# Pages
# ---------------------------------------------------------------
BASE_STYLE="""
body{background:#000;color:#00aaff;font-family:Consolas,monospace;text-align:center;padding:30px}
h1{color:#00bfff}
input,select{padding:8px;margin:5px;border:1px solid #00bfff;background:#0a0a0a;color:#00bfff}
button{background:#00bfff;border:none;padding:12px 25px;font-weight:bold;color:#000;cursor:pointer;border-radius:6px;margin:10px}
#log-box{margin-top:25px;width:90%;max-width:800px;margin:auto;background:#0a0a0a;border:1px solid #00bfff;padding:15px;text-align:left;height:400px;overflow-y:auto;border-radius:10px}
nav a{color:#00bfff;margin:0 10px;text-decoration:none}
"""

@app.route("/")
def home():
    html=f"""
    <html><head><style>{BASE_STYLE}</style></head><body>
    <nav><a href="/">Home</a>|<a href="/about">About</a>|<a href="/help">Help</a>|<a href="/previous">Previous</a></nav>
    <h1>Business Lead Scraper</h1>
    <p>Enter a ZIP code and optional radius (miles)</p>
    <input id='zip' placeholder='ZIP or City' value='Richmond,VA'>
    <input id='rad' type='number' placeholder='Radius (miles)' value='5'>
    <br><button onclick="run()">Start Search</button>
    <div id='log-box'></div>
    <script>
    async function run(){{
        let z=document.getElementById('zip').value;
        let r=document.getElementById('rad').value;
        document.getElementById('log-box').innerHTML='<div>🚀 Running...</div>';
        await fetch(`/run?loc=${{encodeURIComponent(z)}}&r=${{r}}`);
    }}
    async function getLogs(){{
        let r=await fetch('/logs');let j=await r.json();
        let b=document.getElementById('log-box');
        b.innerHTML=j.logs.map(x=>"<div>"+x+"</div>").join('');
        b.scrollTop=b.scrollHeight;
    }}
    setInterval(getLogs,2000);
    </script></body></html>
    """
    return html

@app.route("/run")
def run_route():
    loc=request.args.get("loc","Richmond,VA")
    threading.Thread(target=run_scraper,args=(loc,)).start()
    return jsonify({"started":True,"loc":loc})

@app.route("/logs")
def logs(): return jsonify({"logs":scraper_logs})

@app.route("/previous")
def prev():
    Path("data/exports").mkdir(parents=True, exist_ok=True)
    files=list(Path("data/exports").glob("*.xlsx"))
    out="<h1 style='color:#00bfff'>Previous Exports</h1>"
    if not files: out+="<p>No files yet.</p>"
    else:
        for f in files: out+=f"<p><a style='color:#00bfff' href='/download/{f.name}'>{f.name}</a></p>"
    return f"<html><head><style>{BASE_STYLE}</style></head><body>{out}</body></html>"

@app.route("/download/<n>")
def dl(n):
    p=f"data/exports/{n}"
    return send_file(p,as_attachment=True) if os.path.exists(p) else jsonify({"error":"not found"}),404

@app.route("/about")
def about():
    return f"<html><head><style>{BASE_STYLE}</style></head><body><h1>About</h1><p>Tool for gathering business leads via Google Maps API and auto-syncing to Brevo.</p></body></html>"

@app.route("/help")
def help():
    return f"<html><head><style>{BASE_STYLE}</style></head><body><h1>Help</h1><p>Enter a ZIP or city and radius then click Start. Logs stream live; results auto-upload and export to Excel.</p></body></html>"

if __name__=="__main__":
    app.run(host="0.0.0.0",port=10000)

