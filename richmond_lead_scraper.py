import os, requests, json, re, time, threading
import pandas as pd
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

def should_stop():
    return STOP_EVENT.is_set()

def log_message(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    scraper_logs.append(line)
    if len(scraper_logs) > 400:
        scraper_logs.pop(0)

def get_businesses_from_google(cat, zipcode, radius_miles):
    if should_stop():
        return []
    radius_meters = int(radius_miles) * 1609
    query = f"{cat} near {zipcode}"
    url = f"https://maps.googleapis.com/maps/api/place/textsearch/json?query={query}&radius={radius_meters}&key={GOOGLE_API_KEY}"
    log_message(f"🔎 Searching {cat} near {zipcode} ({radius_miles} mi radius)...")
    try:
        r = requests.get(url, timeout=10).json()
    except Exception as e:
        log_message(f"⚠️ Google search error: {e}")
        return []
    results = r.get("results", [])
    log_message(f"📍 Retrieved {len(results)} {cat} results total.")
    out = []
    for res in results:
        if should_stop():
            break
        name = res.get("name", "Unknown")
        pid = res.get("place_id")
        if not pid:
            continue
        try:
            det = requests.get(
                f"https://maps.googleapis.com/maps/api/place/details/json?place_id={pid}&fields=name,website,formatted_phone_number&key={GOOGLE_API_KEY}",
                timeout=10
            ).json().get("result", {})
        except Exception:
            det = {}
        out.append({
            "name": name if name and name != "Unknown" else res.get("vicinity", "Unknown"),
            "website": det.get("website", ""),
            "phone": det.get("formatted_phone_number", "")
        })
        time.sleep(0.2)
    return out

def find_email_on_website(url):
    if not url:
        return ""
    try:
        r = requests.get(url, timeout=6)
        em = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", r.text)
        for e in em:
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
        for ln in txt.split("."):
            if any(k in ln.lower() for k in ["owner","ceo","founder","manager","director","president"]):
                nm = re.search(r"\b([A-Z][a-z]+ [A-Z][a-z]+)\b", ln)
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
        log_message("⚠️ Already running. Please wait.")
        return
    scraper_in_progress = True
    scraper_logs.clear()
    seen_emails.clear()
    STOP_EVENT.clear()

    log_message("🚀 Scraper started.")

    MAX_RUNTIME = 600   # seconds
    MIN_RESULTS = 50
    start = time.time()

    timer = threading.Timer(MAX_RUNTIME, STOP_EVENT.set)
    timer.start()

    results, uploaded = [], 0

    try:
        for c in categories:
            if should_stop() and len(results) >= MIN_RESULTS:
                log_message(f"⏰ Timeout reached after {len(results)} results — stopping early.")
                break
            results.extend(get_businesses_from_google(c, zipcode, radius))
            if len(results) >= 2000:
                log_message("🔒 Result cap reached. Proceeding to upload.")
                break

        if len(results) < MIN_RESULTS and not should_stop():
            log_message(f"⚠️ Only {len(results)} results found — continuing until minimum reached.")
            for c in categories[:3]:
                if should_stop() and len(results) >= MIN_RESULTS:
                    break
                results.extend(get_businesses_from_google(c, zipcode, radius))
                if len(results) >= MIN_RESULTS:
                    break

        for biz in results[:400]:
            if should_stop() and uploaded >= MIN_RESULTS:
                log_message("⏰ Timeout hit during upload — wrapping up.")
                break

            email = find_email_on_website(biz["website"])
            owner, phone = find_owner_name_and_phone(biz["website"])
            contact = {"name": biz["name"], "phone": phone or biz.get("phone", ""), "website": biz["website"], "email": email, "owner_name": owner}
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
            time.sleep(0.4)

        os.makedirs("runs", exist_ok=True)
        fname = f"runs/run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        pd.DataFrame(results).to_excel(fname, index=False)
        log_message(f"📁 Saved as {fname}")
        log_message(f"🎯 Finished — {uploaded} uploaded ({len(results)} scraped).")
    finally:
        timer.cancel()
        STOP_EVENT.clear()
        scraper_in_progress = False
