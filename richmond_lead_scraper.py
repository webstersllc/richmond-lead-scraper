import os, requests, json, re, time, threading, pandas as pd
from datetime import datetime
from flask import Flask, render_template_string, request, jsonify

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
BREVO_API_KEY = os.getenv("BREVO_API_KEY")

if not GOOGLE_API_KEY or not BREVO_API_KEY:
    raise ValueError("Missing GOOGLE_API_KEY or BREVO_API_KEY")

app = Flask(__name__)
scraper_logs, seen_emails = [], set()
scraper_in_progress = False
STOP_EVENT = threading.Event()

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def log(msg):
    t = datetime.now().strftime("%H:%M:%S")
    line = f"[{t}] {msg}"
    print(line)
    scraper_logs.append(line)
    if len(scraper_logs) > 400: scraper_logs.pop(0)

def should_stop(): return STOP_EVENT.is_set()

def get_businesses_from_google(cat, zipcode, radius):
    if should_stop(): return []
    radius_m = int(radius)*1609
    q = f"{cat} near {zipcode}"
    url = f"https://maps.googleapis.com/maps/api/place/textsearch/json?query={q}&radius={radius_m}&key={GOOGLE_API_KEY}"
    log(f"🔎 Searching {cat} near {zipcode} ({radius} mi)…")
    try: data = requests.get(url, timeout=10).json()
    except Exception as e:
        log(f"⚠️ Google error: {e}"); return []
    res = data.get("results", [])
    log(f"📍 Retrieved {len(res)} {cat} results.")
    out=[]
    for r in res:
        if should_stop(): break
        pid=r.get("place_id");  name=r.get("name","Unknown")
        det={}
        if pid:
            try:
                det=requests.get(
                    f"https://maps.googleapis.com/maps/api/place/details/json?place_id={pid}&fields=name,website,formatted_phone_number&key={GOOGLE_API_KEY}",
                    timeout=10).json().get("result",{})
            except: pass
        out.append({
            "name": name if name!="Unknown" else r.get("vicinity","Unknown"),
            "website": det.get("website",""),
            "phone": det.get("formatted_phone_number","")
        })
        time.sleep(0.25)
    return out

def find_email(url):
    if not url: return ""
    try:
        r=requests.get(url,timeout=6)
        for e in re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",r.text):
            if not any(b in e for b in["example","wixpress","sentry","schema"]): return e
    except: pass
    return ""

def find_owner_phone(url):
    if not url: return "",""
    try:
        r=requests.get(url,timeout=6)
        txt=re.sub(r"<[^>]*>"," ",r.text)
        txt=re.sub(r"\s+"," ",txt)
        for ln in txt.split("."):
            if any(k in ln.lower() for k in["owner","ceo","founder","manager","director","president"]):
                nm=re.search(r"\b([A-Z][a-z]+ [A-Z][a-z]+)\b",ln)
                ph=re.search(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}",txt)
                return (nm.group(1) if nm else ""), (ph.group(0) if ph else "")
        ph=re.search(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}",txt)
        return "", (ph.group(0) if ph else "")
    except: return "",""

def add_to_brevo(c,has_email=True):
    url="https://api.brevo.com/v3/contacts"
    headers={"accept":"application/json","content-type":"application/json","api-key":BREVO_API_KEY}
    payload={
        "email": c.get("email") if has_email else f"{c['name'].replace(' ','').lower()}@placeholder.com",
        "attributes":{
            "FIRSTNAME": c.get("owner_name") or c.get("name"),
            "COMPANY": c.get("name"),
            "PHONE": c.get("phone",""),
            "WEBSITE": c.get("website","")
        },
        "listIds":[3 if has_email else 5]
    }
    r=requests.post(url,headers=headers,data=json.dumps(payload))
    log(f"Added to Brevo (List {'3' if has_email else '5'}): {c.get('email','no email')} ({r.status_code})")

# ------------------------------------------------------------
# Scraper core
# ------------------------------------------------------------
def run_scraper(cats,zipc,rad):
    global scraper_in_progress
    if scraper_in_progress: return log("⚠️ Already running.")
    scraper_in_progress=True; scraper_logs.clear(); seen_emails.clear(); STOP_EVENT.clear()
    log("🚀 Scraper started.")
    MAX=600; MIN=50; timer=threading.Timer(MAX,STOP_EVENT.set); timer.start()
    results=[]; uploaded=0
    try:
        for c in cats:
            if should_stop() and len(results)>=MIN: break
            results+=get_businesses_from_google(c,zipc,rad)
            if len(results)>=2000: break
        for b in results[:400]:
            if should_stop() and uploaded>=MIN: break
            email=find_email(b["website"]); owner,ph=find_owner_phone(b["website"])
            contact={"name":b["name"],"phone":ph or b.get("phone",""),"website":b["website"],"email":email,"owner_name":owner}
            if email and email in seen_emails: log(f"⚠️ Duplicate skipped: {email}"); continue
            if email:
                add_to_brevo(contact,True); seen_emails.add(email); uploaded+=1; log(f"✅ {b['name']} ({email}) → List 3")
            else:
                add_to_brevo(contact,False); uploaded+=1; log(f"📇 {b['name']} (No Email) → List 5")
            time.sleep(0.4)
        os.makedirs("runs",exist_ok=True)
        fn=f"runs/run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        pd.DataFrame(results).to_excel(fn,index=False)
        log(f"📁 Saved as {fn}"); log(f"🎯 Finished — {uploaded} uploaded ({len(results)} scraped).")
    finally:
        timer.cancel(); STOP_EVENT.clear(); scraper_in_progress=False

# ------------------------------------------------------------
# Interface
# ------------------------------------------------------------
@app.route("/")
def home():
    html="""
    <!DOCTYPE html><html><head><meta charset='utf-8'><title>Business Lead Scraper</title>
    <style>
      body{background:#000;color:#00bfff;font-family:Consolas,monospace;text-align:center;padding:30px}
      h1{color:#00bfff} .group{margin:20px}
      .btn{background:#00bfff;color:#000;border:none;padding:10px 16px;border-radius:6px;margin:5px;cursor:pointer;font-weight:bold}
      .btn:hover{background:#0088cc}
      input{padding:8px;margin:5px;border-radius:5px;border:none}
      #log-box{margin-top:25px;border:1px solid #00bfff;border-radius:10px;width:90%;max-width:800px;margin:auto;
               padding:10px;text-align:left;background:#0a0a0a;height:400px;overflow-y:auto}
    </style></head><body>
      <h1>Business Lead Scraper</h1>
      <div>
        <input id='zip' placeholder='ZIP Code' value='23005'>
        <input id='rad' placeholder='Radius (mi)' value='30'>
      </div>
      <div id='cats'>
        <div class='group'><b>Home Services</b><br>
          <button class='btn' onclick='toggle(this)'>Landscaping</button>
          <button class='btn' onclick='toggle(this)'>HVAC</button>
          <button class='btn' onclick='toggle(this)'>Plumbing</button>
          <button class='btn' onclick='toggle(this)'>Roofing</button>
          <button class='btn' onclick='toggle(this)'>Cleaning</button></div>
        <div class='group'><b>Food & Drink</b><br>
          <button class='btn' onclick='toggle(this)'>Restaurants</button>
          <button class='btn' onclick='toggle(this)'>Coffee Shops</button>
          <button class='btn' onclick='toggle(this)'>Bars</button></div>
        <div class='group'><b>Entertainment</b><br>
          <button class='btn' onclick='toggle(this)'>Event Venues</button>
          <button class='btn' onclick='toggle(this)'>Escape Rooms</button>
          <button class='btn' onclick='toggle(this)'>Mini Golf</button></div>
        <div class='group'><b>Professional Services</b><br>
          <button class='btn' onclick='toggle(this)'>Insurance Agencies</button>
          <button class='btn' onclick='toggle(this)'>Law Firms</button>
          <button class='btn' onclick='toggle(this)'>Real Estate Agents</button>
          <button class='btn' onclick='toggle(this)'>Accountants</button></div>
        <div class='group'><b>Health & Wellness</b><br>
          <button class='btn' onclick='toggle(this)'>Gyms</button>
          <button class='btn' onclick='toggle(this)'>Chiropractors</button>
          <button class='btn' onclick='toggle(this)'>Dentists</button></div>
        <div class='group'><b>Automotive</b><br>
          <button class='btn' onclick='toggle(this)'>Auto Repair</button>
          <button class='btn' onclick='toggle(this)'>Car Dealers</button>
          <button class='btn' onclick='toggle(this)'>Car Wash</button></div>
      </div>
      <button class='btn' onclick='runScraper()'>Run Scraper</button>
      <div id='log-box'></div>
    <script>
      let sel=[];
      function toggle(b){const t=b.innerText;
        if(sel.includes(t)){sel=sel.filter(x=>x!==t);b.style.background='#00bfff'}
        else{sel.push(t);b.style.background='#0088cc'}}
      async function runScraper(){
        document.getElementById('log-box').innerHTML="<div>🚀 Scraper starting…</div>";
        const z=document.getElementById('zip').value,r=document.getElementById('rad').value;
        await fetch('/run',{method:'POST',headers:{'Content-Type':'application/json'},
          body:JSON.stringify({cats:sel,zip:z,rad:r})});
      }
      async function logs(){const r=await fetch('/logs');const d=await r.json();
        const lb=document.getElementById('log-box');
        lb.innerHTML=d.logs.map(l=>"<div>"+l+"</div>").join('');lb.scrollTop=lb.scrollHeight;}
      setInterval(logs,2000);
    </script></body></html>
    """
    return render_template_string(html)

@app.route("/run",methods=["POST"])
def run_route():
    d=request.get_json()
    threading.Thread(target=run_scraper,args=(d.get("cats",["Landscaping"]),d.get("zip","23005"),d.get("rad",30))).start()
    return jsonify({"status":"started"})

@app.route("/logs")
def logs(): return jsonify({"logs":scraper_logs})

if __name__=="__main__":
    app.run(host="0.0.0.0",port=10000)

