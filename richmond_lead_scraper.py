import os, re, json, time, requests, pandas as pd
from bs4 import BeautifulSoup
from flask import Flask

# --------------------------------------------------
#  CONFIG
# --------------------------------------------------
BREVO_API_KEY = os.getenv("BREVO_API_KEY")
if not BREVO_API_KEY:
    raise ValueError("Missing BREVO_API_KEY environment variable")

CITY = "Richmond VA"
SEARCH_TERMS = [
    "new small businesses near " + CITY,
    "recently opened businesses in " + CITY,
    "local startups in " + CITY,
    "marketing agencies in " + CITY,
    "restaurants in " + CITY,
    "contractors in " + CITY,
    "coffee shops in " + CITY,
    "salons in " + CITY,
    "HVAC companies in " + CITY,
    "plumbers in " + CITY,
]
HEADERS = {"User-Agent": "Mozilla/5.0"}
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}")
NAME_KEYS = ["owner", "manager", "founder", "ceo", "director"]

# --------------------------------------------------
#  SCRAPER FUNCTIONS
# --------------------------------------------------
def search_links(term, max_links=25):
    print(f"🔎  Searching Google for: {term}")
    url = f"https://www.google.com/search?q={term}"
    html = requests.get(url, headers=HEADERS).text
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("http") and "google" not in href:
            links.append(href)
    # remove duplicates & obvious directories
    bad = ["yelp", "facebook", "linkedin", "tripadvisor", "bbb.org"]
    clean = [l for l in links if not any(b in l for b in bad)]
    return list(dict.fromkeys(clean))[:max_links]

def scrape_site(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=8)
        text = BeautifulSoup(r.text, "html.parser").get_text(" ", strip=True)
        emails = list(set(re.findall(EMAIL_RE, text)))
        phones = list(set(re.findall(PHONE_RE, text)))
        owner = ""
        for line in text.splitlines():
            if any(k in line.lower() for k in NAME_KEYS):
                owner = line.strip()[:60]
                break
        title = BeautifulSoup(r.text, "html.parser").title
        biz = title.string.strip() if title else "Unknown Business"
        return {"business_name": biz, "owner_name": owner,
                "emails": emails, "phones": phones, "website": url}
    except Exception as e:
        print(f"❌ Error scraping {url}: {e}")
        return None

def add_to_brevo(contact):
    if not contact["emails"]:
        return
    url = "https://api.brevo.com/v3/contacts"
    headers = {"accept": "application/json",
               "content-type": "application/json",
               "api-key": BREVO_API_KEY}
    data = {
        "email": contact["emails"][0],
        "attributes": {
            "FIRSTNAME": contact.get("owner_name", ""),
            "COMPANY": contact.get("business_name", ""),
            "PHONE": contact["phones"][0] if contact["phones"] else "",
            "WEBSITE": contact["website"],
        },
        "listIds": [3],
    }
    r = requests.post(url, headers=headers, data=json.dumps(data))
    print(f"📬  Sent to Brevo: {contact['emails'][0]} ({r.status_code})")

# --------------------------------------------------
#  MAIN WORKFLOW
# --------------------------------------------------
def main():
    print("🚀 Starting lead run...")
    leads, uploaded = set(), 0
    max_leads = 25

    for term in SEARCH_TERMS:
        if uploaded >= max_leads:
            break
        sites = search_links(term)
        for site in sites:
            if uploaded >= max_leads:
                break
            info = scrape_site(site)
            if not info or not info["emails"]:
                continue
            email = info["emails"][0].lower()
            if email in leads:
                continue
            leads.add(email)
            add_to_brevo(info)
            uploaded += 1
            print(f"✅ {uploaded}/{max_leads}: {email}")
            time.sleep(2)  # mild delay
    print(f"🎯 Finished. {uploaded} leads uploaded.")

# --------------------------------------------------
#  FLASK APP (for Render)
# --------------------------------------------------
app = Flask(__name__)

@app.route("/")
def home():
    return "Richmond Lead Scraper active."

@app.route("/run")
def run_scraper():
    print("🟢 /run triggered")
    main()
    print("🏁 Done")
    return "Scraper completed successfully."

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)



