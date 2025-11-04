import os, time, json, re, threading, requests
from flask import Flask, render_template_string, jsonify, request
from datetime import datetime
from urllib.parse import urljoin

# --------------------------------------------------------------------
# Environment Variables
# --------------------------------------------------------------------
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
BREVO_API_KEY = os.getenv("BREVO_API_KEY")
if not GOOGLE_API_KEY or not BREVO_API_KEY:
    raise ValueError("Missing GOOGLE_API_KEY or BREVO_API_KEY")

# --------------------------------------------------------------------
# Flask + Storage
# --------------------------------------------------------------------
app = Flask(__name__)
scraper_logs = []
seen_emails = set()

def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    entry = f"[{ts}] {msg}"
    print(entry)
    scraper_logs.append(entry)
    if len(scraper_logs) > 400:
        scraper_logs.pop(0)

# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------
def find_owner_and_phone(url):
    if not url: return "", ""
    try:
        resp = requests.get(url, timeout=6)
        html = resp.text
        text = re.sub(r"<[^>]*>", " ", html)
        text = re.sub(r"\s+", " ", text)
        # look for about page
        about = re.search(r'href=["\'](.*?about[^"\']*)["\']', html, re.I)
        if about:
            link = urljoin(url, about.group(1))
            try:
                t = requests.get(link, timeout=5).text
                text += " " + re.sub(r"<[^>]*>", " ", t)
            except: pass
        owner=""
        for line in text.split("."):
            if any(k in line.lower() for k in ["owner","founder","ceo","manager","director","president"]):
                m=re.search(r"\b([A-Z][a-z]+ [A-Z][a-z]+)\b", line)
                if m: owner=m.group(1); break
        phone=""
        m=re.search(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", text)
        if m: phone=m.group(0)
        return owner,phone
    except Exception as e:
        log(f"Error parsing {url}: {e}")
        return "",""

def find_email(url):
    if not url: return ""
    try:
        resp=requests.get(url,timeout=6)
        emails=re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",resp.text)
        for e in emails:
            if not any(bad in e for bad in ["example.","schema.org","wixpress","sentry"]):
                return e
    except Exception as e:
        log(f"Error scanning {url}: {e}")
    return ""

def add_to_brevo(c):
    if not c.get("email"): return
    url="https://api.brevo.com/v3/contacts"
    headers={"accept":"application/json","content-type":"application/json","api-key":BREVO_API_KEY}
    data={
        "email":c["email"],
        "attributes":{
            "FIRSTNAME": c.get("owner_name") or c.get("name",""),
            "COMPANY": c.get("name",""),
            "PHONE": c.get("phone",""),
            "WEBSITE": c.get("website","")
        },
        "listIds":[3]
    }
    r=requests.post(url,headers=headers,data=json.dumps(data))
    log(f"Added {c['email']} to Brevo ({r.status_code})")

# --------------------------------------------------------------------
# Google Places with pagination (up to 60)
# --------------------------------------------------------------------
def get_places(biz_type,zip_code,radius_mi):
    radius_m=int(radius_mi*1609)
    all_results=[]
    query=f"{biz_type}+near+{zip_code}"
    url=f"https://maps.googleapis.com/maps/api/place/textsearch/json?query={query}&radius={radius_m}&key={GOOGLE_API_KEY}"
    while url and len(all_results)<60:
        r=requests.get(url).json()
        results=r.get("results",[])
        all_results.extend(results)
        token=r.get("next_page_token")
        if token:
            time.sleep(2)
            url=f"https://maps.googleapis.com/maps/api/place/textsearch/json?pagetoken={token}&key={GOOGLE_API_KEY}"
        else: break
    return all_results[:60]

# --------------------------------------------------------------------
# Main Scraper Logic
# --------------------------------------------------------------------
def run_scraper_process(types,zip_code,radius):
    scraper_logs.clear()
    log("🚀 Starting lead scraper...")
    uploaded=0
    for btype in types:
        log(f"📍 Searching {btype} in {zip_code} ({radius} miles)...")
        places=get_places(btype,zip_code,radius)
        log(f"Found {len(places)} businesses from Google.")
        for p in places:
            name=p.get("name","")
            pid=p.get("place_id")
            det=requests.get(
                f"https://maps.googleapis.com/maps/api/place/details/json?place_id={pid}&fields=name,website,formatted_phone_number&key={GOOGLE_API_KEY}"
            ).json().get("result",{})
            site=det.get("website","")
            phone=det.get("formatted_phone_number","")
            email=find_email(site)
            if not email:
                log(f"❌ No email found for {name}.")
                continue
            if email in seen_emails:
                log(f"⚠️ Duplicate skipped: {name} ({email})")
                continue
            owner,ph=find_owner_and_phone(site)
            contact={"name":name,"website":site,"phone":ph or phone,"email":email,"owner_name":owner or name}
            add_to_brevo(contact)
            seen_emails.add(email)
            uploaded+=1
            log(f"✅ {name} ({email}) added with owner: {owner or name}")
            time.sleep(1.5)
    log(f"🎯 Scraper finished — {uploaded} unique contacts uploaded to Brevo.")

# --------------------------------------------------------------------
# Web Interface
# --------------------------------------------------------------------
@app.route("/")
def index():
    return render_template_string("""
<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><title>Richmond Lead Scraper</title>
<style>
body{background:#000;color:#00aaff;font-family:Consolas,monospace;text-align:center;padding:30px;}
h1{font-size:2.4em;color:#00bfff;margin-bottom:10px;}
button{background:#00bfff;border:none;padding:10px 18px;margin:4px;font-size:15px;font-weight:bold;color:#000;cursor:pointer;border-radius:6px;box-shadow:0 0 10px #00bfff;}
button:hover{background:#0088cc;transform:scale(1.05);}
.biz-btn{background:#001933;border:1px solid #00bfff;color:#00bfff;}
.biz-btn.active{background:#00bfff;color:#000;}
#business-buttons{display:flex;flex-wrap:wrap;justify-content:center;gap:8px;margin-bottom:15px;}
input{padding:8px;margin:5px;border-radius:4px;border:1px solid #00bfff;background:#000;color:#00bfff;}
#log-box{margin-top:25px;width:90%;max-width:800px;margin-left:auto;margin-right:auto;background:#0a0a0a;border:1px solid #00bfff;
padding:20px;text-align:left;height:400px;overflow-y:auto;border-radius:10px;}
.log-entry{margin:4px 0;}
</style></head>
<body>
<div id="control-section">
  <h1>Richmond Lead Scraper</h1>
  <div id="business-buttons">
    {% for b in ['HVAC','Landscaping','Restaurants','Coffee Shops','Gyms & Fitness','Salons & Spas','Realtors','Plumbers','Automotive','Retail','Construction','Cleaning','Law Firms','Daycare','Pet Services','Medical'] %}
    <button class="biz-btn" onclick="toggleBusiness(this,'{{b}}')">{{b}}</button>
    {% endfor %}
  </div>
  <div>
    <input id="zip" placeholder="ZIP Code" value="23220">
    <input id="radius" placeholder="Radius (miles)" value="15" type="number" min="1" max="50">
    <button onclick="runSearch()">Start Scraper</button>
  </div>
</div>
<div id="log-box" style="display:none;"></div>

<script>
let selectedBusinesses=[];
function toggleBusiness(btn,type){
  if(btn.classList.contains("active")){
    btn.classList.remove("active");
    selectedBusinesses=selectedBusinesses.filter(b=>b!==type);
  }else{
    btn.classList.add("active");
    selectedBusinesses.push(type);
  }
}
async function runSearch(){
  const zip=document.getElementById("zip").value||"23220";
  const radius=document.getElementById("radius").value||15;
  const query=selectedBusinesses.join(",");
  document.getElementById("control-section").style.display="none";
  const box=document.getElementById("log-box");
  box.style.display="block";
  box.innerHTML="<div>🚀 Scraper starting...</div>";
  fetch(`/run?types=${encodeURIComponent(query)}&zip=${zip}&radius=${radius}`);
}
async function fetchLogs(){
  const res=await fetch('/logs');
  const data=await res.json();
  const box=document.getElementById('log-box');
  box.innerHTML=data.logs.map(l=>"<div class='log-entry'>"+l+"</div>").join('');
  box.scrollTop=box.scrollHeight;
}
setInterval(fetchLogs,2000);
</script>
</body></html>
""")

@app.route("/run")
def run_scraper_route():
    biz_types=request.args.get("types","business").split(",")
    zip_code=request.args.get("zip","23220")
    radius=float(request.args.get("radius",15))
    t=threading.Thread(target=run_scraper_process,args=(biz_types,zip_code,radius))
    t.start()
    return jsonify({"status":"Scraper started","types":biz_types})

@app.route("/logs")
def get_logs():
    return jsonify({"logs":scraper_logs})

if __name__=="__main__":
    app.run(host="0.0.0.0",port=10000)

