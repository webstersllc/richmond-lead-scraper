import os, re, json, time, requests
from bs4 import BeautifulSoup
from flask import Flask, render_template_string, jsonify

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
BREVO_API_KEY = os.getenv("BREVO_API_KEY")
if not BREVO_API_KEY:
    raise ValueError("Missing BREVO_API_KEY environment variable")

CITY = "Richmond VA"
SEARCH_TERMS = [
    "restaurants",
    "contractors",
    "marketing agencies",
    "coffee shops",
    "plumbers",
    "electricians",
    "salons",
    "HVAC companies",
    "real estate agents",
    "roofing companies"
]
HEADERS = {"User-Agent": "Mozilla/5.0"}
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}")

# --------------------------------------------------
# SCRAPER HELPERS
# --------------------------------------------------

def search_links(term, max_links=15):
    """Search YellowPages for business listings in the target city"""
    print(f"📒 Searching YellowPages for: {term}")
    city_query = CITY.replace(" ", "%20")
    url = f"https://www.yellowpages.com/search?search_terms={term.replace(' ', '%20')}&geo_location_terms={city_query}"
    r = requests.get(url, headers=HEADERS, timeout=10)
    soup = BeautifulSoup(r.text, "html.parser")
    links = []
    for a in soup.select("a.business-name[href]"):
        href = a["href"]
        if href.startswith("/"):
            href = "https://www.yellowpages.com" + href
        links.append(href)
    print(f"Found {len(links)} listings for {term}")
    return links[:max_links]


def find_contact_page(domain):
    """Try to find contact or about page for extra email addresses"""
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


def scrape_yellowpages_listing(url):
    """Scrape an individual YellowPages listing for business info"""
    try:
        r = requests.get(url, headers=HEADERS, timeout=8)
        soup = BeautifulSoup(r.text, "html.parser")

        name = soup.select_one("h1.business-name")
        name = name.get_text(strip=True) if name else "Unknown Business"

        phone = soup.select_one("p.phone")
        phone = phone.get_text(strip=True) if phone else ""

        text = soup.get_text(" ", strip=True)
        emails = list(set(re.findall(EMAIL_RE, text)))

        # Try contact/about pages if no email found
        if not emails:
            emails = find_contact_page(url)

        website = ""
        link_tag = soup.select_one("a.primary-btn")
        if link_tag and link_tag.get("href", "").startswith("http"):
            website = link_tag["href"]

        return {
            "business_name": name,
            "emails": emails,
            "phones": [phone] if phone else [],
            "website": website or url,
        }
    except Exception as e:
        print(f"❌ Error scraping {url}: {e}")
        return None


def add_to_brevo(contact):
    """Send collected lead to Brevo via API"""
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
    print(f"📬 Sent to Brevo: {contact['emails'][0]} ({r.status_code})")


# --------------------------------------------------
# MAIN SCRAPER LOGIC
# --------------------------------------------------

def run_scraper_process(max_leads=25):
    print("🚀 Starting Lead Scraper Run")
    leads = set()
    uploaded = 0

    for term in SEARCH_TERMS:
        if uploaded >= max_leads:
            break

        listings = search_links(term)
        for listing in listings:
            if uploaded >= max_leads:
                break

            info = scrape_yellowpages_listing(listing)
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
# FLASK APP FRONTEND
# --------------------------------------------------

app = Flask(__name__)
last_run_count = 0

@app.route("/")
def home():
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

