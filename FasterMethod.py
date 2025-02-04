#!/usr/bin/env python3
import asyncio
import argparse
import csv
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# Global variables that will be set via command-line arguments
START_URL = None
CSV_FILENAME = None

# Constants
FIELDNAMES = ["team", "state", "detail_url", "club_name", "club_website"]
PAGE_LOAD_TIMEOUT = 30000  # 30 seconds
CONCURRENCY_LIMIT = 50     # Increase to 50 concurrent detail page tasks
BATCH_SIZE = 500           # Checkpoint after processing 500 clubs

# ---------------------------
# CSV Helper Functions
# ---------------------------
def write_header():
    with open(CSV_FILENAME, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
        writer.writeheader()

def append_records(records):
    if records:
        with open(CSV_FILENAME, "a", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
            writer.writerows(records)
            csvfile.flush()

# ---------------------------
# Helper: safe_get (with retries)
# ---------------------------
async def safe_get(page, url, retries=3, delay=5):
    for attempt in range(retries):
        try:
            await page.goto(url, timeout=PAGE_LOAD_TIMEOUT)
            return True
        except Exception as e:
            print(f"Attempt {attempt+1} to load {url} failed: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(delay)
            else:
                print("Maximum retries reached for URL:", url)
                return False

# ---------------------------
# Listing Page Extraction Functions
# ---------------------------
async def extract_listing_data(page):
    """Extracts (team_name, detail_url, state) from the current listing page."""
    await page.wait_for_selector("table tbody tr", timeout=PAGE_LOAD_TIMEOUT)
    rows = await page.query_selector_all("table tbody tr")
    listing_data = []
    for row in rows:
        try:
            team_link_elem = await row.query_selector("td:nth-child(3) a")
            if not team_link_elem:
                continue
            team_name = (await team_link_elem.inner_text()).strip()
            team_href = await team_link_elem.get_attribute("href")
            if team_href and not team_href.startswith("http"):
                team_href = "https://rankings.gotsport.com" + team_href

            state_td = await row.query_selector("td:nth-child(5)")
            state = None
            if state_td:
                state_span = await state_td.query_selector("span")
                if state_span:
                    state = (await state_span.inner_text()).strip()
            listing_data.append((team_name, team_href, state))
        except Exception as e:
            print("Error extracting listing row:", e)
    print(f"Extracted {len(listing_data)} teams from this page.")
    return listing_data

async def go_to_next_page(page, current_page_number):
    """Clicks the numeric pagination button for the next page."""
    next_page_number = current_page_number + 1
    try:
        await page.wait_for_selector("div.mx-auto.max-w-7xl.px-4.sm\\:px-6.lg\\:px-8", timeout=PAGE_LOAD_TIMEOUT)
        xpath_next = f"//button[normalize-space(text())='{next_page_number}']"
        next_button = await page.wait_for_selector(xpath_next, timeout=5000)
        if next_button:
            print(f"Clicking page {next_page_number} button.")
            await next_button.click()
            await page.wait_for_selector("table tbody tr", timeout=PAGE_LOAD_TIMEOUT)
            await asyncio.sleep(2)
            return True
    except PlaywrightTimeoutError as te:
        print(f"No next page button for page {next_page_number} (timeout): {te}")
    except Exception as e:
        print("Error clicking next page button:", e)
    return False

# ---------------------------
# Detail Page Extraction Functions
# ---------------------------
async def extract_club_info(page):
    """Extracts the club name and website from a team detail page."""
    try:
        await page.wait_for_selector("//div[span[text()='Club Information']]", timeout=PAGE_LOAD_TIMEOUT)
    except PlaywrightTimeoutError as te:
        print("Detail container not found:", te)
        return None, None

    snippet = await page.content()
    print("Detail page snippet (first 500 characters):", snippet[:500])

    club_name = None
    try:
        club_name_elem = await page.wait_for_selector("//span[text()='Club Name']/following-sibling::span[1]", timeout=5000)
        if club_name_elem:
            club_name = (await club_name_elem.inner_text()).strip()
            print("Club Name:", club_name)
    except Exception as e:
        print("Could not extract club name:", e)

    club_website = None
    try:
        club_website_elem = await page.wait_for_selector("//span[text()='Website']/following-sibling::span//a", timeout=5000)
        if club_website_elem:
            club_website = await club_website_elem.get_attribute("href")
            print("Club Website:", club_website)
    except Exception as e:
        print("Could not extract club website:", e)
    
    return club_name, club_website

async def process_team_detail(team_tuple, browser):
    """Opens a new browser context/page for a team detail page, extracts info, and returns a record."""
    team_name, detail_url, state = team_tuple
    print(f"\n=== Processing Detail for Team: {team_name} ===")
    context_detail = await browser.new_context()
    page_detail = await context_detail.new_page()
    record = {
        "team": team_name,
        "state": state,
        "detail_url": detail_url,
        "club_name": None,
        "club_website": None
    }
    loaded = await safe_get(page_detail, detail_url)
    if not loaded:
        print(f"Failed to load detail page for {team_name}")
        await context_detail.close()
        return record
    try:
        club_name, club_website = await extract_club_info(page_detail)
        record["club_name"] = club_name
        record["club_website"] = club_website
    except Exception as e:
        print(f"Error processing detail for team {team_name}: {e}")
    finally:
        await context_detail.close()
    return record

# ---------------------------
# Phase 1 – Collect All Club URLs
# ---------------------------
async def collect_club_urls():
    """Navigates through all listing pages and collects a list of (team, detail_url, state) tuples."""
    all_listing_data = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        print("Loading starting URL...")
        await safe_get(page, START_URL)
        await page.wait_for_selector("table tbody tr", timeout=PAGE_LOAD_TIMEOUT)
        current_page = 1
        while True:
            print(f"\n--- Processing Listing Page {current_page} ---")
            listing_data = await extract_listing_data(page)
            all_listing_data.extend(listing_data)
            if not await go_to_next_page(page, current_page):
                print("Reached last listing page.")
                break
            current_page += 1
        await context.close()
        await browser.close()
    return all_listing_data

# ---------------------------
# Phase 2 – Process Detail Pages in Batches with Checkpointing
# ---------------------------
async def process_details_in_batches(all_listing_data, batch_size=BATCH_SIZE):
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    all_results = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        async def process_with_semaphore(team_tuple):
            async with semaphore:
                return await process_team_detail(team_tuple, browser)
        total = len(all_listing_data)
        for i in range(0, total, batch_size):
            batch = all_listing_data[i:i+batch_size]
            print(f"\nProcessing batch {i // batch_size + 1} (clubs {i+1} to {i+len(batch)})...")
            try:
                batch_results = await asyncio.gather(*(process_with_semaphore(team) for team in batch))
            except Exception as e:
                print("Exception during batch processing:", e)
                batch_results = []
            append_records(batch_results)
            all_results.extend(batch_results)
            print(f"Checkpoint: Saved {len(batch_results)} records to CSV.")
        await browser.close()
    return all_results

# ---------------------------
# Process a Single Site (one start URL)
# ---------------------------
async def process_site(start_url, output):
    global START_URL, CSV_FILENAME
    START_URL = start_url
    CSV_FILENAME = output
    write_header()
    print(f"Processing site: {start_url}")
    all_listing_data = await collect_club_urls()
    print(f"Collected {len(all_listing_data)} club URLs from listings for {start_url}.")
    results = await process_details_in_batches(all_listing_data)
    print(f"Scraping complete for {start_url}. Total records processed: {len(results)}.")

# ---------------------------
# Command-Line Argument Parsing
# ---------------------------
def parse_arguments():
    parser = argparse.ArgumentParser(description="Club Info Scraper")
    parser.add_argument(
        '--start_urls',
        type=str,
        nargs=2,
        required=True,
        help="Two starting URLs to process concurrently (each instance handles 2 URLs)."
    )
    parser.add_argument(
        '--outputs',
        type=str,
        nargs=2,
        required=True,
        help="Two CSV file names corresponding to each starting URL."
    )
    return parser.parse_args()

# ---------------------------
# Main Function
# ---------------------------
async def main():
    args = parse_arguments()
    tasks = []
    # Create a task for each URL/output pair
    for url, out in zip(args.start_urls, args.outputs):
        tasks.append(asyncio.create_task(process_site(url, out)))
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())



# import asyncio
# import csv
# import time
# import logging
# from urllib.parse import urlparse, parse_qs
# from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# # ---------------------------
# # Logging Configuration
# # ---------------------------
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] %(message)s",
#     datefmt="%Y-%m-%d %H:%M:%S",
# )

# # ---------------------------
# # Configuration and Constants
# # ---------------------------
# BASE_URLS = [
#     # "https://rankings.gotsport.com/?team_country=USA&age=11&gender=m",
#     # "https://rankings.gotsport.com/?team_country=USA&age=12&gender=m",
#     # "https://rankings.gotsport.com/?team_country=USA&age=13&gender=m",
#     "https://rankings.gotsport.com/?team_country=USA&age=14&gender=m",
#     "https://rankings.gotsport.com/?team_country=USA&age=15&gender=m",
#     "https://rankings.gotsport.com/?team_country=USA&age=16&gender=m",
#     "https://rankings.gotsport.com/?team_country=USA&age=17&gender=m",
#     "https://rankings.gotsport.com/?team_country=USA&age=18&gender=m",
#     "https://rankings.gotsport.com/?team_country=USA&age=19&gender=m",
#     "https://rankings.gotsport.com/?team_country=USA&age=10&gender=f",
#     "https://rankings.gotsport.com/?team_country=USA&age=11&gender=f",
#     "https://rankings.gotsport.com/?team_country=USA&age=12&gender=f",
#     "https://rankings.gotsport.com/?team_country=USA&age=13&gender=f",
#     "https://rankings.gotsport.com/?team_country=USA&age=14&gender=f",
#     "https://rankings.gotsport.com/?team_country=USA&age=15&gender=f",
#     "https://rankings.gotsport.com/?team_country=USA&age=16&gender=f",
#     "https://rankings.gotsport.com/?team_country=USA&age=17&gender=f",
#     "https://rankings.gotsport.com/?team_country=USA&age=18&gender=f",
#     "https://rankings.gotsport.com/?team_country=USA&age=19&gender=f",
# ]
# PAGE_LOAD_TIMEOUT = 30000  # 30 seconds
# CONCURRENCY_LIMIT = 50     # Limit concurrent detail processing
# BATCH_SIZE = 500           # Process and save every 500 teams from a site

# # ---------------------------
# # CSV Helper Functions
# # ---------------------------
# def get_csv_filename(base_url):
#     """Derives a CSV filename from the base URL's query parameters (age and gender)."""
#     parsed = urlparse(base_url)
#     qs = parse_qs(parsed.query)
#     age = qs.get("age", ["unknown"])[0]
#     gender = qs.get("gender", ["unknown"])[0]
#     return f"{age}{gender}club_info.csv"

# def write_header(csv_filename):
#     with open(csv_filename, "w", newline="", encoding="utf-8") as csvfile:
#         writer = csv.DictWriter(csvfile, fieldnames=["base_url", "team", "state", "detail_url", "club_name", "club_website"])
#         writer.writeheader()
#     logging.info(f"CSV header written to {csv_filename}.")

# def append_records(records, csv_filename):
#     if records:
#         with open(csv_filename, "a", newline="", encoding="utf-8") as csvfile:
#             writer = csv.DictWriter(csvfile, fieldnames=["base_url", "team", "state", "detail_url", "club_name", "club_website"])
#             writer.writerows(records)
#             csvfile.flush()
#         logging.info(f"Appended {len(records)} records to {csv_filename}.")

# # ---------------------------
# # Helper: safe_get (with retries)
# # ---------------------------
# async def safe_get(page, url, retries=3, delay=5):
#     for attempt in range(retries):
#         try:
#             logging.info(f"Navigating to {url} (attempt {attempt+1})")
#             await page.goto(url, timeout=PAGE_LOAD_TIMEOUT)
#             logging.info(f"Successfully loaded {url}")
#             return True
#         except Exception as e:
#             logging.warning(f"Attempt {attempt+1} to load {url} failed: {e}")
#             if attempt < retries - 1:
#                 await asyncio.sleep(delay)
#             else:
#                 screenshot_path = f"screenshot_timeout_{int(time.time())}.png"
#                 try:
#                     await page.screenshot(path=screenshot_path)
#                     logging.info(f"Saved screenshot to {screenshot_path} for URL: {url}")
#                 except Exception as ss_e:
#                     logging.error(f"Failed to take screenshot for URL {url}: {ss_e}")
#                 logging.error(f"Maximum retries reached for URL: {url}")
#                 return False

# # ---------------------------
# # Listing Page Extraction Functions
# # ---------------------------
# async def extract_listing_data(page):
#     """
#     Extracts (team, detail_url, state) from the current listing page.
#     """
#     await page.wait_for_selector("table tbody tr", timeout=PAGE_LOAD_TIMEOUT)
#     rows = await page.query_selector_all("table tbody tr")
#     listing_data = []
#     for row in rows:
#         try:
#             team_link_elem = await row.query_selector("td:nth-child(3) a")
#             if not team_link_elem:
#                 continue
#             team_name = (await team_link_elem.inner_text()).strip()
#             team_href = await team_link_elem.get_attribute("href")
#             if team_href and not team_href.startswith("http"):
#                 team_href = "https://rankings.gotsport.com" + team_href
#             state_td = await row.query_selector("td:nth-child(5)")
#             state = None
#             if state_td:
#                 state_span = await state_td.query_selector("span")
#                 if state_span:
#                     state = (await state_span.inner_text()).strip()
#             listing_data.append((team_name, team_href, state))
#         except Exception as e:
#             logging.error(f"Error extracting listing row: {e}")
#     logging.info(f"Extracted {len(listing_data)} teams from this page.")
#     return listing_data

# async def go_to_next_page(page, current_page_number):
#     """
#     Clicks the numeric pagination button for the next page.
#     Returns True if successful, else False.
#     """
#     next_page_number = current_page_number + 1
#     try:
#         await page.wait_for_selector("div.mx-auto.max-w-7xl.px-4.sm\\:px-6.lg\\:px-8", timeout=PAGE_LOAD_TIMEOUT)
#         xpath_next = f"//button[normalize-space(text())='{next_page_number}']"
#         next_button = await page.wait_for_selector(xpath_next, timeout=5000)
#         if next_button:
#             logging.info(f"Clicking page {next_page_number} button.")
#             await next_button.click()
#             await page.wait_for_selector("table tbody tr", timeout=PAGE_LOAD_TIMEOUT)
#             await asyncio.sleep(2)
#             return True
#     except PlaywrightTimeoutError as te:
#         logging.warning(f"No next page button for page {next_page_number} (timeout): {te}")
#     except Exception as e:
#         logging.error(f"Error clicking next page button: {e}")
#     return False

# # ---------------------------
# # Detail Page Extraction Functions
# # ---------------------------
# async def extract_club_info(page):
#     """
#     Extracts club name and club website from a team detail page.
#     Returns (club_name, club_website).
#     """
#     try:
#         await page.wait_for_selector("//div[span[normalize-space(text())='Club Information']]", timeout=PAGE_LOAD_TIMEOUT)
#     except PlaywrightTimeoutError as te:
#         logging.error(f"Detail container not found: {te}")
#         return None, None

#     content = await page.content()
#     logging.info(f"Detail page snippet (first 500 chars): {content[:500]}")

#     club_name = None
#     try:
#         club_name_elem = await page.wait_for_selector("//span[normalize-space(text())='Club Name']/following-sibling::span[1]", timeout=5000)
#         if club_name_elem:
#             club_name = (await club_name_elem.inner_text()).strip()
#             logging.info(f"Club Name: {club_name}")
#     except Exception as e:
#         logging.error(f"Could not extract club name: {e}")

#     club_website = None
#     try:
#         club_website_elem = await page.wait_for_selector("//span[normalize-space(text())='Website']/following-sibling::span//a", timeout=5000)
#         if club_website_elem:
#             club_website = await club_website_elem.get_attribute("href")
#             logging.info(f"Club Website: {club_website}")
#     except Exception as e:
#         logging.error(f"Could not extract club website: {e}")

#     return club_name, club_website

# async def process_team_detail(team_tuple, browser, base_url):
#     """
#     Opens a new browser context/page for a team detail page, extracts club info,
#     and returns a record dictionary that includes the base_url.
#     """
#     team_name, detail_url, state = team_tuple
#     logging.info(f"=== Processing Detail for Team: {team_name} ===")
#     context_detail = await browser.new_context()
#     page_detail = await context_detail.new_page()
#     record = {
#         "base_url": base_url,
#         "team": team_name,
#         "state": state,
#         "detail_url": detail_url,
#         "club_name": None,
#         "club_website": None
#     }
#     loaded = await safe_get(page_detail, detail_url)
#     if not loaded:
#         logging.error(f"Failed to load detail page for {team_name}")
#         await context_detail.close()
#         return record
#     try:
#         club_name, club_website = await extract_club_info(page_detail)
#         record["club_name"] = club_name
#         record["club_website"] = club_website
#     except Exception as e:
#         logging.error(f"Error processing detail for team {team_name}: {e}")
#     finally:
#         await context_detail.close()
#     return record

# # ---------------------------
# # Process a Single Site (Base URL)
# # ---------------------------
# async def process_site(base_url):
#     """
#     Processes one rankings site (base URL) by:
#       - Navigating through listing pages.
#       - Extracting team detail URLs.
#       - Processing detail pages in batches (checkpoint after every BATCH_SIZE teams).
#     Each site's results are written to its own CSV.
#     """
#     csv_filename = get_csv_filename(base_url)
#     write_header(csv_filename)
#     site_start_time = time.time()
#     batch_listing_data = []  # Accumulates teams for detail processing.
#     current_page = 1

#     logging.info(f"Starting processing for site: {base_url}")
#     async with async_playwright() as p:
#         browser = await p.chromium.launch(headless=True)
#         context = await browser.new_context()
#         page = await context.new_page()

#         if not await safe_get(page, base_url):
#             logging.error(f"Could not load site: {base_url}")
#             return

#         await page.wait_for_selector("table tbody tr", timeout=PAGE_LOAD_TIMEOUT)

#         while True:
#             logging.info(f"--- Processing Listing Page {current_page} for site: {base_url} ---")
#             listing_data = await extract_listing_data(page)
#             batch_listing_data.extend(listing_data)
#             if not await go_to_next_page(page, current_page):
#                 logging.info("Reached last listing page for site.")
#                 break
#             current_page += 1

#             if len(batch_listing_data) >= BATCH_SIZE:
#                 batch_start = time.time()
#                 logging.info(f"Checkpoint: Processing details for {len(batch_listing_data)} teams from site {base_url}...")
#                 semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
#                 async def process_with_semaphore(team):
#                     async with semaphore:
#                         return await process_team_detail(team, browser, base_url)
#                 batch_results = await asyncio.gather(*(process_with_semaphore(team) for team in batch_listing_data))
#                 append_records(batch_results, csv_filename)
#                 logging.info(f"Checkpoint: Saved {len(batch_results)} records in {time.time()-batch_start:.2f} seconds.")
#                 batch_listing_data.clear()

#         if batch_listing_data:
#             batch_start = time.time()
#             logging.info(f"Processing remaining {len(batch_listing_data)} teams for site {base_url}...")
#             semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
#             async def process_with_semaphore(team):
#                 async with semaphore:
#                     return await process_team_detail(team, browser, base_url)
#             remaining_results = await asyncio.gather(*(process_with_semaphore(team) for team in batch_listing_data))
#             append_records(remaining_results, csv_filename)
#             logging.info(f"Saved remaining {len(remaining_results)} records in {time.time()-batch_start:.2f} seconds.")
#             batch_listing_data.clear()

#         await context.close()
#         await browser.close()
#     logging.info(f"Finished processing site: {base_url} in {time.time()-site_start_time:.2f} seconds.")

# # ---------------------------
# # Main Async Function: Process All Sites Concurrently
# # ---------------------------
# async def main():
#     overall_start = time.time()
#     tasks = [process_site(url) for url in BASE_URLS]
#     await asyncio.gather(*tasks)
#     logging.info(f"Scraping complete. Overall time: {time.time()-overall_start:.2f} seconds.")

# if __name__ == "__main__":
#     # On macOS, use: caffeinate -i python3 your_script.py
#     asyncio.run(main())


# # import asyncio
# # import csv
# # import json
# # from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# # # ---------------------------
# # # Configuration and Constants
# # # ---------------------------
# # START_URL = "https://rankings.gotsport.com/?team_country=USA&age=10&gender=m"
# # CSV_FILENAME = "club_info.csv"
# # FIELDNAMES = ["team", "state", "detail_url", "club_name", "club_website"]
# # PAGE_LOAD_TIMEOUT = 30000  # 30 seconds
# # CONCURRENCY_LIMIT = 10     # Adjust as needed
# # BATCH_SIZE = 500           # Save checkpoint every 500 clubs

# # # ---------------------------
# # # CSV Helper Functions
# # # ---------------------------
# # def write_header():
# #     with open(CSV_FILENAME, "w", newline="", encoding="utf-8") as csvfile:
# #         writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
# #         writer.writeheader()

# # def append_records(records):
# #     if records:
# #         with open(CSV_FILENAME, "a", newline="", encoding="utf-8") as csvfile:
# #             writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
# #             writer.writerows(records)
# #             csvfile.flush()

# # # ---------------------------
# # # Helper: safe_get (with retries)
# # # ---------------------------
# # async def safe_get(page, url, retries=3, delay=5):
# #     for attempt in range(retries):
# #         try:
# #             await page.goto(url, timeout=PAGE_LOAD_TIMEOUT)
# #             return True
# #         except Exception as e:
# #             print(f"Attempt {attempt+1} to load {url} failed: {e}")
# #             if attempt < retries - 1:
# #                 await asyncio.sleep(delay)
# #             else:
# #                 print("Maximum retries reached for URL:", url)
# #                 return False

# # # ---------------------------
# # # Listing Page Extraction Functions
# # # ---------------------------
# # async def extract_listing_data(page):
# #     """Extracts (team_name, detail_url, state) from the current listing page."""
# #     await page.wait_for_selector("table tbody tr", timeout=PAGE_LOAD_TIMEOUT)
# #     rows = await page.query_selector_all("table tbody tr")
# #     listing_data = []
# #     for row in rows:
# #         try:
# #             team_link_elem = await row.query_selector("td:nth-child(3) a")
# #             if not team_link_elem:
# #                 continue
# #             team_name = (await team_link_elem.inner_text()).strip()
# #             team_href = await team_link_elem.get_attribute("href")
# #             if team_href and not team_href.startswith("http"):
# #                 team_href = "https://rankings.gotsport.com" + team_href

# #             state_td = await row.query_selector("td:nth-child(5)")
# #             state = None
# #             if state_td:
# #                 state_span = await state_td.query_selector("span")
# #                 if state_span:
# #                     state = (await state_span.inner_text()).strip()
# #             listing_data.append((team_name, team_href, state))
# #         except Exception as e:
# #             print("Error extracting listing row:", e)
# #     print(f"Extracted {len(listing_data)} teams from this page.")
# #     return listing_data

# # async def go_to_next_page(page, current_page_number):
# #     """Clicks the numeric pagination button for the next page."""
# #     next_page_number = current_page_number + 1
# #     try:
# #         await page.wait_for_selector("div.mx-auto.max-w-7xl.px-4.sm\\:px-6.lg\\:px-8", timeout=PAGE_LOAD_TIMEOUT)
# #         xpath_next = f"//button[normalize-space(text())='{next_page_number}']"
# #         next_button = await page.wait_for_selector(xpath_next, timeout=5000)
# #         if next_button:
# #             print(f"Clicking page {next_page_number} button.")
# #             await next_button.click()
# #             await page.wait_for_selector("table tbody tr", timeout=PAGE_LOAD_TIMEOUT)
# #             await asyncio.sleep(2)
# #             return True
# #     except PlaywrightTimeoutError as te:
# #         print(f"No next page button for page {next_page_number} (timeout): {te}")
# #     except Exception as e:
# #         print("Error clicking next page button:", e)
# #     return False

# # # ---------------------------
# # # Detail Page Extraction Functions
# # # ---------------------------
# # async def extract_club_info(page):
# #     """Extracts the club name and website from a team detail page."""
# #     try:
# #         await page.wait_for_selector("//div[span[text()='Club Information']]", timeout=PAGE_LOAD_TIMEOUT)
# #     except PlaywrightTimeoutError as te:
# #         print("Detail container not found:", te)
# #         return None, None

# #     snippet = await page.content()
# #     print("Detail page snippet (first 500 characters):", snippet[:500])

# #     club_name = None
# #     try:
# #         club_name_elem = await page.wait_for_selector("//span[text()='Club Name']/following-sibling::span[1]", timeout=5000)
# #         if club_name_elem:
# #             club_name = (await club_name_elem.inner_text()).strip()
# #             print("Club Name:", club_name)
# #     except Exception as e:
# #         print("Could not extract club name:", e)

# #     club_website = None
# #     try:
# #         club_website_elem = await page.wait_for_selector("//span[text()='Website']/following-sibling::span//a", timeout=5000)
# #         if club_website_elem:
# #             club_website = await club_website_elem.get_attribute("href")
# #             print("Club Website:", club_website)
# #     except Exception as e:
# #         print("Could not extract club website:", e)
    
# #     return club_name, club_website

# # async def process_team_detail(team_tuple, browser):
# #     """Opens a new browser context/page for a team detail page, extracts info, and returns a record."""
# #     team_name, detail_url, state = team_tuple
# #     print(f"\n=== Processing Detail for Team: {team_name} ===")
# #     context_detail = await browser.new_context()
# #     page_detail = await context_detail.new_page()
# #     record = {
# #         "team": team_name,
# #         "state": state,
# #         "detail_url": detail_url,
# #         "club_name": None,
# #         "club_website": None
# #     }
# #     loaded = await safe_get(page_detail, detail_url)
# #     if not loaded:
# #         print(f"Failed to load detail page for {team_name}")
# #         await context_detail.close()
# #         return record
# #     try:
# #         club_name, club_website = await extract_club_info(page_detail)
# #         record["club_name"] = club_name
# #         record["club_website"] = club_website
# #     except Exception as e:
# #         print(f"Error processing detail for team {team_name}: {e}")
# #     finally:
# #         await context_detail.close()
# #     return record

# # # ---------------------------
# # # Phase 1 – Collect All Club URLs
# # # ---------------------------
# # async def collect_club_urls():
# #     """Navigates through all listing pages and collects a list of (team, detail_url, state) tuples."""
# #     all_listing_data = []
# #     async with async_playwright() as p:
# #         browser = await p.chromium.launch(headless=True)
# #         context = await browser.new_context()
# #         page = await context.new_page()

# #         print("Loading starting URL...")
# #         await safe_get(page, START_URL)
# #         await page.wait_for_selector("table tbody tr", timeout=PAGE_LOAD_TIMEOUT)

# #         current_page = 1
# #         while True:
# #             print(f"\n--- Processing Listing Page {current_page} ---")
# #             listing_data = await extract_listing_data(page)
# #             all_listing_data.extend(listing_data)
# #             if not await go_to_next_page(page, current_page):
# #                 print("Reached last listing page.")
# #                 break
# #             current_page += 1

# #         await context.close()
# #         await browser.close()
# #     return all_listing_data

# # # ---------------------------
# # # Phase 2 – Process Detail Pages in Batches with Checkpointing
# # # ---------------------------
# # async def process_details_in_batches(all_listing_data, batch_size=BATCH_SIZE):
# #     """
# #     Processes detail pages in batches, appending the results to the CSV after each batch.
# #     This way, even if the process stalls, progress is saved.
# #     """
# #     semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
# #     all_results = []
    
# #     async with async_playwright() as p:
# #         browser = await p.chromium.launch(headless=True)
        
# #         async def process_with_semaphore(team_tuple):
# #             async with semaphore:
# #                 return await process_team_detail(team_tuple, browser)
        
# #         total = len(all_listing_data)
# #         for i in range(0, total, batch_size):
# #             batch = all_listing_data[i:i+batch_size]
# #             print(f"\nProcessing batch {i // batch_size + 1} (clubs {i+1} to {i+len(batch)})...")
# #             try:
# #                 batch_results = await asyncio.gather(*(process_with_semaphore(team) for team in batch))
# #             except Exception as e:
# #                 print("Exception during batch processing:", e)
# #                 batch_results = []  # In case of a fatal error, continue to next batch.
# #             append_records(batch_results)
# #             all_results.extend(batch_results)
# #             print(f"Checkpoint: Saved {len(batch_results)} records to CSV.")
        
# #         await browser.close()
# #     return all_results

# # # ---------------------------
# # # Main Async Function with Checkpointing
# # # ---------------------------
# # async def main():
# #     write_header()  # Write CSV header at the start.
    
# #     # Phase 1: Collect all club URLs from the listings.
# #     all_listing_data = await collect_club_urls()
# #     print(f"\nCollected {len(all_listing_data)} club URLs from listings.")
    
# #     # Phase 2: Process detail pages in batches and checkpoint.
# #     detail_results = await process_details_in_batches(all_listing_data)
# #     print(f"\nScraping complete. Total records processed: {len(detail_results)}. Records saved to {CSV_FILENAME}.")

# # if __name__ == "__main__":
# #     asyncio.run(main())
