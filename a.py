
import requests
from bs4 import BeautifulSoup
import sqlite3
import schedule
import time
import hashlib
import logging
import random

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

BASE_URL = 'https://norac.co.ke'
DB_FILE = 'norac_projects.db'

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

HEADERS = {
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Connection": "keep-alive",
}

def get(url):
    headers = HEADERS.copy()
    headers["User-Agent"] = random.choice(USER_AGENTS)
    try:
        time.sleep(random.uniform(2.0, 4.0))
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        return response
    except requests.RequestException as e:
        logging.error(f"Request failed for {url}: {e}")
        return None

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS projects (
            list_id TEXT PRIMARY KEY,
            title TEXT,
            price TEXT,
            status TEXT,
            url TEXT,
            hash TEXT
        )
    ''')
    conn.commit()
    conn.close()

def get_listings():
    response = get(f'{BASE_URL}/projects')
    if not response:
        return []
    soup = BeautifulSoup(response.text, 'html.parser')
    listings = []
    for a in soup.select('a.project-card-link'):
        href = a.get('href')
        if href and href.startswith('/'):
            listings.append(BASE_URL + href)
    return listings

def get_project_details(url):
    response = get(url)
    if not response:
        return None

    soup = BeautifulSoup(response.text, 'html.parser')
    try:
        list_id_element = soup.select_one('div.property-meta-item span')
        if list_id_element and "REF NO:" in list_id_element.text:
            list_id = list_id_element.text.split("REF NO:")[-1].strip()
        else:
            list_id = "N/A"

        title_element = soup.select_one('h2.property-title')
        title = title_element.text.strip() if title_element else "No Title"

        price_element = soup.select_one('span.property-price')
        price = price_element.text.strip() if price_element else "No Price"

        status_element = soup.select_one('div.property-labels span.label-status')
        status = status_element.text.strip() if status_element else "N/A"

        hash_content = hashlib.md5((title + price + status).encode()).hexdigest()

        return {
            'list_id': list_id,
            'title': title,
            'price': price,
            'status': status,
            'url': url,
            'hash': hash_content
        }

    except Exception as e:
        logging.warning(f"Failed to parse {url}: {e}")
        return None

def save_or_update(project):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT hash FROM projects WHERE list_id = ?", (project['list_id'],))
    result = c.fetchone()

    if result is None:
        logging.info(f"New project added: {project['title']} (ID: {project['list_id']})")
        c.execute('''
            INSERT INTO projects (list_id, title, price, status, url, hash)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (project['list_id'], project['title'], project['price'], project['status'], project['url'], project['hash']))
    elif result[0] != project['hash']:
        logging.info(f"Project updated: {project['title']} (ID: {project['list_id']})")
        c.execute('''
            UPDATE projects
            SET title = ?, price = ?, status = ?, url = ?, hash = ?
            WHERE list_id = ?
        ''', (project['title'], project['price'], project['status'], project['url'], project['hash'], project['list_id']))
    else:
        logging.debug(f"No change in project: {project['title']} (ID: {project['list_id']})")

    conn.commit()
    conn.close()

def scrape_and_save():
    logging.info("Starting scrape...")
    listings = get_listings()
    if not listings:
        logging.warning("No listings found.")
        return

    logging.info(f"Found {len(listings)} listings.")
    for url in listings:
        project = get_project_details(url)
        if project:
            save_or_update(project)
        else:
            logging.warning(f"Could not retrieve project at {url}")
    logging.info("Scrape finished.")

def start_scheduler():
    schedule.every(1).hours.do(scrape_and_save)
    logging.info("Scheduler started. Scraping every hour.")
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    init_db()
    scrape_and_save()
    start_scheduler()
