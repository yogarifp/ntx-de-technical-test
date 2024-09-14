import httpx
from bs4 import BeautifulSoup
import polars as pl
import json
import os
from tqdm import tqdm
import asyncio

BASE_URL = "https://www.fortiguard.com/encyclopedia?type=ips&risk={level}&page={i}"
MAX_PAGES = [5, 5, 5, 5, 5]  # Contoh jumlah halaman max untuk setiap level
DATA_DIR = "datasets"

# Create dir
os.makedirs(DATA_DIR, exist_ok=True)

async def fetch_page(client, level, page):
    url = BASE_URL.format(level=level, i=page)
    try:
        response = await client.get(url)
        response.raise_for_status()
        return response.text
    except httpx.RequestError as e:
        print(f"Request error for level {level} page {page}: {e}")
        return None
    except httpx.HTTPStatusError as e:
        print(f"HTTP error for level {level} page {page}: {e}")
        return None

async def scrape_level(level, max_pages):
    async with httpx.AsyncClient() as client:
        tasks = [fetch_page(client, level, page) for page in range(1, max_pages + 1)]
        responses = await asyncio.gather(*tasks)
        return responses

def parse_html(html):
    soup = BeautifulSoup(html, 'html.parser')
    data = []
    for item in soup.select('.article-item'):  # Sesuaikan selector dengan struktur HTML situs web
        title = item.select_one('.title').get_text(strip=True)
        link = item.select_one('a')['href']
        data.append({'title': title, 'link': link})
    return data

def save_to_csv(data, level):
    df = pl.DataFrame(data)
    df.write_csv(f"{DATA_DIR}/forti_lists_{level}.csv")

async def main():
    skipped_pages = {}
    for level, max_pages in enumerate(MAX_PAGES, start=1):
        print(f"Scraping level {level}...")
        responses = await scrape_level(level, max_pages)
        all_data = []
        skipped = []
        for page, html in enumerate(responses, start=1):
            if html is None:
                skipped.append(page)
                continue
            data = parse_html(html)
            all_data.extend(data)
        save_to_csv(all_data, level)
        if skipped:
            skipped_pages[level] = skipped

    if skipped_pages:
        with open(f"{DATA_DIR}/skipped.json", "w") as f:
            json.dump(skipped_pages, f, indent=4)

if __name__ == "__main__":
    asyncio.run(main())
