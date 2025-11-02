import os, re, json, time, requests
from bs4 import BeautifulSoup
from flask import Flask, render_template_string, jsonify

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
BAD_DOMAINS = ["yelp", "facebook", "linkedin", "tripadvisor", "bbb.org"]

# --------------------------------------------------
#  SCRAPER HELPERS
# --------------------------------------------------
def search_links(term, max_links=15):
    print(f"🔎  Searching Google for: {term}")
    html = requests.get(f"https://www.google.com/search?q={term}", headers=HEADERS).text
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("http") and "google" not in href and not any(b in href for b in BAD_DOMAINS):
            links.append(href)
    return list(dict.fromkeys(links))[:max_links]


def scrape_site(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=8)
        text = BeautifulSoup(r.text, "html.parser").get_text(" ", strip=True)
        emails = list(set(re.findall(EMAIL_RE, text)))
        if not emails:
            # Try contact pages if no direct email found
            emails = find_contact_page(url)
        phones = list(set(re.findall(PHONE_RE, text)))
        title = BeautifulSoup(r.text, "html.parser").title
        biz = title.string.strip() if title else "Unknown Business"
        return {"business_name": biz, "emails": emails, "phones": phones, "website": url}
    except Exception as e:
        print(f"❌ Error scraping {url}: {e}")
        return None


def find_contact_page(domain):
    possible = ["/contact", "/contact-us", "/about", "/about-us"]
    found = []
    for path in possible:
        test_url = domain.rstrip("/") + path
        try:
            r = requests.get(test_url, headers=HEADERS, timeout=6)
            if r.status_code == 200 and "@" in r.text:
                found += re.findall(EMAIL_RE, r.text)
        except:
            continue
    return list(set(found))


def add_to_brevo(contact):
    if not contact["emails"]:
        return
    url = "https://api.brevo.com/v3/contacts"
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "api-key": BREVO_API_KEY,
    }
    data = {
        "email": contact["emails"][0],
        "attributes": {
            "COMPANY": contact["business_name"],
            "PHONE": contact["phones"][0] if contact["phones"] else "",
            "WEBSITE": contact["website"],
        },
        "listIds": [3],
    }
    r = requests.post(url, headers=headers, data=json.dumps(data))
    print(f"📬  Sent to Brevo: {contact['emails'][0]} ({r.status_code})")


# --------------------------------------------------
#  MAIN SCRAPER
# --------------------------------------------------
def run_scraper_process(max_leads=25):
    print("🚀 Starting Lead Scraper Run")
    leads = set()
    uploaded = 0

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
            time.sleep(2)

    print(f"🎯 Finished. {uploaded} leads uploaded.")
    return uploaded


# --------------------------------------------------
#  FLASK APP
# --------------------------------------------------
app = Flask(__name__)
last_run_count = 0

@app.route("/")
def home():
    # Simple HTML page with button + counter
    html = f"""
    <!doctype html>
    <html>
    <head>
        <title>Richmond Lead Scraper</title>
        <style>
            body {{ font-family: Arial; text-align: center; margin-top: 100px; }}
            button {{
                padding: 12px 25px; background-color: #007bff; color: white;
                border: none; border-radius: 6px; font-size: 18px; cursor: pointer;
            }}
            button:hover {{ background-color: #0056b3; }}
            h1 {{ color: #222; }}
            p {{ font-size: 20px; }}
        </style>
    </head>
    <body>
        <h1>Richmond Lead Scraper</h1>
        <p>Last Run Collected: <b id="count">{last_run_count}</b> leads</p>
        <button onclick="runScraper()">Run Scraper</button>
        <p id="status"></p>

        <script>
            function runScraper() {{
                document.getElementById('status').innerText = "Running scraper... please wait.";
                fetch('/run')
                    .then(res => res.json())
                    .then(data => {{
                        document.getElementById('status').innerText = "✅ Completed!";
                        document.getElementById('count').innerText = data.count;
                    }})
                    .catch(err => {{
                        document.getElementById('status').innerText = "❌ Error running scraper.";
                    }});
            }}
        </script>
    </body>
    </html>
    """
    return render_template_string(html)


@app.route("/run")
def run_scraper_route():
    global last_run_count
    print("🟢 /run triggered")
    count = run_scraper_process()
    last_run_count = count
    print("🏁 Done")
    return jsonify({"message": "Scraper completed successfully.", "count": count})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)

