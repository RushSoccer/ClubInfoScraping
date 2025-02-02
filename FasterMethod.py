import asyncio
import csv
import json
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ---------------------------
# Configuration and Constants
# ---------------------------
START_URL = "https://rankings.gotsport.com/?team_country=USA&age=10&gender=m"
CSV_FILENAME = "club_info.csv"
FIELDNAMES = ["team", "state", "detail_url", "club_name", "club_website"]
PAGE_LOAD_TIMEOUT = 30000  # 30 seconds
CONCURRENCY_LIMIT = 10     # Adjust as needed
BATCH_SIZE = 500           # Save checkpoint every 500 clubs

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
    """
    Processes detail pages in batches, appending the results to the CSV after each batch.
    This way, even if the process stalls, progress is saved.
    """
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
                batch_results = []  # In case of a fatal error, continue to next batch.
            append_records(batch_results)
            all_results.extend(batch_results)
            print(f"Checkpoint: Saved {len(batch_results)} records to CSV.")
        
        await browser.close()
    return all_results

# ---------------------------
# Main Async Function with Checkpointing
# ---------------------------
async def main():
    write_header()  # Write CSV header at the start.
    
    # Phase 1: Collect all club URLs from the listings.
    all_listing_data = await collect_club_urls()
    print(f"\nCollected {len(all_listing_data)} club URLs from listings.")
    
    # Phase 2: Process detail pages in batches and checkpoint.
    detail_results = await process_details_in_batches(all_listing_data)
    print(f"\nScraping complete. Total records processed: {len(detail_results)}. Records saved to {CSV_FILENAME}.")

if __name__ == "__main__":
    asyncio.run(main())

# import asyncio
# import csv
# from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# # ---------------------------
# # Configuration and Constants
# # ---------------------------
# START_URL = "https://rankings.gotsport.com/?team_country=USA&age=10&gender=m"
# CSV_FILENAME = "club_info.csv"
# FIELDNAMES = ["team", "state", "detail_url", "club_name", "club_website"]
# PAGE_LOAD_TIMEOUT = 30000  # 30 seconds
# CONCURRENCY_LIMIT = 10     # Adjust based on your system and site tolerance
# BATCH_SIZE = 500           # Save progress after processing every 500 clubs

# # ---------------------------
# # CSV Helper Functions
# # ---------------------------
# def write_header():
#     with open(CSV_FILENAME, "w", newline="", encoding="utf-8") as csvfile:
#         writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
#         writer.writeheader()

# def append_records(records):
#     with open(CSV_FILENAME, "a", newline="", encoding="utf-8") as csvfile:
#         writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
#         writer.writerows(records)
#         csvfile.flush()

# # ---------------------------
# # Helper: safe_get (with retries)
# # ---------------------------
# async def safe_get(page, url, retries=3, delay=5):
#     for attempt in range(retries):
#         try:
#             await page.goto(url, timeout=PAGE_LOAD_TIMEOUT)
#             return True
#         except Exception as e:
#             print(f"Attempt {attempt+1} to load {url} failed: {e}")
#             if attempt < retries - 1:
#                 await asyncio.sleep(delay)
#             else:
#                 print("Maximum retries reached for URL:", url)
#                 return False

# # ---------------------------
# # Listing Page Extraction Functions
# # ---------------------------
# async def extract_listing_data(page):
#     """
#     Extracts (team_name, detail_url, state) from the current listing page.
#     """
#     await page.wait_for_selector("table tbody tr", timeout=PAGE_LOAD_TIMEOUT)
#     rows = await page.query_selector_all("table tbody tr")
#     listing_data = []
#     for row in rows:
#         try:
#             # Extract team name and detail URL from the 3rd column.
#             team_link_elem = await row.query_selector("td:nth-child(3) a")
#             if not team_link_elem:
#                 continue
#             team_name = (await team_link_elem.inner_text()).strip()
#             team_href = await team_link_elem.get_attribute("href")
#             if team_href and not team_href.startswith("http"):
#                 team_href = "https://rankings.gotsport.com" + team_href

#             # Extract state from the 5th column.
#             state_td = await row.query_selector("td:nth-child(5)")
#             state = None
#             if state_td:
#                 state_span = await state_td.query_selector("span")
#                 if state_span:
#                     state = (await state_span.inner_text()).strip()
#             listing_data.append((team_name, team_href, state))
#         except Exception as e:
#             print("Error extracting listing row:", e)
#     print(f"Extracted {len(listing_data)} teams from this page.")
#     return listing_data

# async def go_to_next_page(page, current_page_number):
#     """
#     Clicks the numeric pagination button for the next page.
#     Returns True if successful, otherwise False.
#     """
#     next_page_number = current_page_number + 1
#     try:
#         await page.wait_for_selector("div.mx-auto.max-w-7xl.px-4.sm\\:px-6.lg\\:px-8", timeout=PAGE_LOAD_TIMEOUT)
#         xpath_next = f"//button[normalize-space(text())='{next_page_number}']"
#         next_button = await page.wait_for_selector(xpath_next, timeout=5000)
#         if next_button:
#             print(f"Clicking page {next_page_number} button.")
#             await next_button.click()
#             await page.wait_for_selector("table tbody tr", timeout=PAGE_LOAD_TIMEOUT)
#             await asyncio.sleep(2)
#             return True
#     except PlaywrightTimeoutError as te:
#         print(f"No next page button for page {next_page_number} (timeout): {te}")
#     except Exception as e:
#         print("Error clicking next page button:", e)
#     return False

# # ---------------------------
# # Detail Page Extraction Functions
# # ---------------------------
# async def extract_club_info(page):
#     """
#     Extracts the club name and website from a team detail page.
#     (This is your original function which worked before.)
#     """
#     try:
#         await page.wait_for_selector("//div[span[text()='Club Information']]", timeout=PAGE_LOAD_TIMEOUT)
#     except PlaywrightTimeoutError as te:
#         print("Detail container not found:", te)
#         return None, None

#     snippet = await page.content()
#     print("Detail page snippet (first 500 characters):", snippet[:500])

#     club_name = None
#     try:
#         club_name_elem = await page.wait_for_selector("//span[text()='Club Name']/following-sibling::span[1]", timeout=5000)
#         if club_name_elem:
#             club_name = (await club_name_elem.inner_text()).strip()
#             print("Club Name:", club_name)
#     except Exception as e:
#         print("Could not extract club name:", e)

#     club_website = None
#     try:
#         club_website_elem = await page.wait_for_selector("//span[text()='Website']/following-sibling::span//a", timeout=5000)
#         if club_website_elem:
#             club_website = await club_website_elem.get_attribute("href")
#             print("Club Website:", club_website)
#     except Exception as e:
#         print("Could not extract club website:", e)
    
#     return club_name, club_website

# async def process_team_detail(team_tuple, browser):
#     """
#     Opens a new browser context/page for a team detail page,
#     extracts club info, and returns a record dictionary.
#     """
#     team_name, detail_url, state = team_tuple
#     print(f"\n=== Processing Detail for Team: {team_name} ===")
#     context_detail = await browser.new_context()
#     page_detail = await context_detail.new_page()
#     record = {
#         "team": team_name,
#         "state": state,
#         "detail_url": detail_url,
#         "club_name": None,
#         "club_website": None
#     }
#     # Use safe_get to try loading the detail page.
#     loaded = await safe_get(page_detail, detail_url)
#     if not loaded:
#         print(f"Failed to load detail page for {team_name}")
#         await context_detail.close()
#         return record
#     try:
#         club_name, club_website = await extract_club_info(page_detail)
#         record["club_name"] = club_name
#         record["club_website"] = club_website
#     except Exception as e:
#         print(f"Error processing detail for team {team_name}: {e}")
#     finally:
#         await context_detail.close()
#     return record

# # ---------------------------
# # Phase 1 – Collect All Club URLs
# # ---------------------------
# async def collect_club_urls():
#     """
#     Navigates through all listing pages and collects a list of (team, detail_url, state) tuples.
#     """
#     all_listing_data = []
#     async with async_playwright() as p:
#         browser = await p.chromium.launch(headless=True)
#         context = await browser.new_context()
#         page = await context.new_page()

#         print("Loading starting URL...")
#         await safe_get(page, START_URL)
#         await page.wait_for_selector("table tbody tr", timeout=PAGE_LOAD_TIMEOUT)

#         current_page = 1
#         while True:
#             print(f"\n--- Processing Listing Page {current_page} ---")
#             listing_data = await extract_listing_data(page)
#             all_listing_data.extend(listing_data)
#             if not await go_to_next_page(page, current_page):
#                 print("Reached last listing page.")
#                 break
#             current_page += 1

#         await context.close()
#         await browser.close()
#     return all_listing_data

# # ---------------------------
# # Phase 2 – Process Detail Pages in Batches with Checkpointing
# # ---------------------------
# async def process_details_in_batches(all_listing_data, batch_size=BATCH_SIZE):
#     """
#     Processes detail pages in batches (default: every 500 clubs),
#     appends the batch results to the CSV, and returns the total records.
#     """
#     semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
#     all_results = []
    
#     async with async_playwright() as p:
#         browser = await p.chromium.launch(headless=True)
        
#         async def process_with_semaphore(team_tuple):
#             async with semaphore:
#                 return await process_team_detail(team_tuple, browser)
        
#         total = len(all_listing_data)
#         for i in range(0, total, batch_size):
#             batch = all_listing_data[i:i+batch_size]
#             print(f"\nProcessing batch {i // batch_size + 1} (clubs {i+1} to {i+len(batch)})...")
#             batch_results = await asyncio.gather(*(process_with_semaphore(team) for team in batch))
#             append_records(batch_results)
#             all_results.extend(batch_results)
#             print(f"Checkpoint: Saved {len(batch_results)} records to CSV.")
        
#         await browser.close()
#     return all_results

# # ---------------------------
# # Main Async Function
# # ---------------------------
# async def main():
#     write_header()  # Write CSV header once.
#     # Phase 1: Collect all club URLs from the listings.
#     all_listing_data = await collect_club_urls()
#     print(f"\nCollected {len(all_listing_data)} club URLs from listings.")
    
#     # Phase 2: Process detail pages in batches (checkpoint every batch).
#     detail_results = await process_details_in_batches(all_listing_data)
#     print(f"\nScraping complete. Total records processed: {len(detail_results)}. Records saved to {CSV_FILENAME}.")

# # ---------------------------
# # Run the Async Main Function.
# # ---------------------------
# if __name__ == "__main__":
#     # On macOS, run with: caffeinate -i python3 your_script.py
#     asyncio.run(main())


# # import asyncio
# # import csv
# # import time
# # from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# # # ---------------------------
# # # Configuration and Constants
# # # ---------------------------
# # START_URL = "https://rankings.gotsport.com/?team_country=USA&age=10&gender=m"
# # CSV_FILENAME = "FASTERclub_info.csv"
# # PAGE_LOAD_TIMEOUT = 20000  # in milliseconds (20 seconds)

# # # ---------------------------
# # # Helper Functions
# # # ---------------------------
# # async def extract_team_urls(page):
# #     """Extracts team names and their detail page URLs from the current listings page."""
# #     # Wait for the table rows to be present.
# #     await page.wait_for_selector("table tbody tr", timeout=PAGE_LOAD_TIMEOUT)
# #     rows = await page.query_selector_all("table tbody tr")
# #     team_data = []
# #     for row in rows:
# #         try:
# #             # The team link is in the third <td>
# #             team_link = await row.query_selector("td:nth-child(3) a")
# #             if team_link:
# #                 team_name = (await team_link.inner_text()).strip()
# #                 team_href = await team_link.get_attribute("href")
# #                 # Ensure the URL is absolute:
# #                 if team_href and not team_href.startswith("http"):
# #                     team_href = "https://rankings.gotsport.com" + team_href
# #                 team_data.append((team_name, team_href))
# #         except Exception as e:
# #             print("Error extracting team link:", e)
# #     print(f"Found {len(team_data)} teams on this page.")
# #     return team_data

# # async def go_to_next_page(page, current_page_number):
# #     """
# #     Clicks the pagination button for page (current_page_number+1) using numeric pagination.
# #     Returns True if clicked; otherwise, False.
# #     """
# #     next_page_number = current_page_number + 1
# #     # The pagination buttons are in a container with several buttons.
# #     try:
# #         # Wait for the pagination container.
# #         await page.wait_for_selector("div.mx-auto.max-w-7xl.px-4.sm\\:px-6.lg\\:px-8", timeout=PAGE_LOAD_TIMEOUT)
# #         # Use an XPath to find a button with normalized text equal to the next page number.
# #         xpath_next = f"//button[normalize-space(text())='{next_page_number}']"
# #         next_button = await page.wait_for_selector(xpath_next, timeout=5000)
# #         if next_button:
# #             print(f"Clicking page {next_page_number} button.")
# #             await next_button.click()
# #             # Wait for the new page to load by waiting for table rows.
# #             await page.wait_for_selector("table tbody tr", timeout=PAGE_LOAD_TIMEOUT)
# #             # Optionally wait a few seconds
# #             await asyncio.sleep(2)
# #             return True
# #     except PlaywrightTimeoutError as te:
# #         print(f"No next page button for page {next_page_number} (timeout): {te}")
# #     except Exception as e:
# #         print(f"Error clicking next page button: {e}")
# #     return False

# async def extract_club_info(page):
#     """Extracts the club name and website from a team detail page."""
#     # Wait for the detail container that includes "Club Information" to appear.
#     try:
#         await page.wait_for_selector("//div[span[text()='Club Information']]", timeout=PAGE_LOAD_TIMEOUT)
#     except PlaywrightTimeoutError as te:
#         print("Detail container not found:", te)
#         return None, None

#     # Optionally print a snippet of the page source for debugging.
#     snippet = await page.content()
#     print("Detail page snippet (first 500 characters):", snippet[:500])

#     # Extract the club name:
#     club_name = None
#     try:
#         club_name_elem = await page.wait_for_selector("//span[text()='Club Name']/following-sibling::span[1]", timeout=5000)
#         if club_name_elem:
#             club_name = (await club_name_elem.inner_text()).strip()
#             print("Club Name:", club_name)
#     except Exception as e:
#         print("Could not extract club name:", e)

#     # Extract the club website:
#     club_website = None
#     try:
#         club_website_elem = await page.wait_for_selector("//span[text()='Website']/following-sibling::span//a", timeout=5000)
#         if club_website_elem:
#             club_website = await club_website_elem.get_attribute("href")
#             print("Club Website:", club_website)
#     except Exception as e:
#         print("Could not extract club website:", e)
    
#     return club_name, club_website

# # ---------------------------
# # Main Async Function
# # ---------------------------
# async def main():
#     all_team_urls = []  # List of (team_name, team_url)
#     current_page = 1
#     async with async_playwright() as p:
#         # Launch browser in headless mode.
#         browser = await p.chromium.launch(headless=True)
#         context = await browser.new_context()
#         page = await context.new_page()

#         # Go to the starting URL.
#         print("Loading starting URL...")
#         await page.goto(START_URL, timeout=PAGE_LOAD_TIMEOUT)
#         await page.wait_for_selector("table tbody tr", timeout=PAGE_LOAD_TIMEOUT)

#         # Pagination loop: collect all team URLs.
#         while True:
#             print(f"\n--- Processing Listing Page {current_page} ---")
#             team_urls = await extract_team_urls(page)
#             all_team_urls.extend(team_urls)
#             # Try to go to the next page.
#             has_next = await go_to_next_page(page, current_page)
#             if not has_next:
#                 print("Reached last page of listings.")
#                 break
#             current_page += 1

#         print(f"\nCollected a total of {len(all_team_urls)} team URLs from all pages.")
#         # Close the listings page.
#         await context.close()

#         # Now, for each team URL, open a new browser context (or page) to extract club info.
#         club_results = []
#         for idx, (team_name, team_url) in enumerate(all_team_urls):
#             print(f"\n=== Processing Detail for Team {idx+1}: {team_name} ===")
#             # Create a new context and page for each detail page
#             context_detail = await browser.new_context()
#             page_detail = await context_detail.new_page()
#             try:
#                 await page_detail.goto(team_url, timeout=PAGE_LOAD_TIMEOUT)
#                 club_name, club_website = await extract_club_info(page_detail)
#                 club_results.append({
#                     "team": team_name,
#                     "club_name": club_name,
#                     "club_website": club_website
#                 })
#             except Exception as e:
#                 print(f"Error processing detail for team {team_name}: {e}")
#             finally:
#                 await context_detail.close()
#             # Optional delay between detail pages.
#             await asyncio.sleep(1)

#         # Save results to CSV.
#         with open(CSV_FILENAME, "w", newline="", encoding="utf-8") as csvfile:
#             fieldnames = ["team", "club_name", "club_website"]
#             writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
#             writer.writeheader()
#             for item in club_results:
#                 writer.writerow(item)
#         print(f"\nScraping complete. {len(club_results)} records saved to {CSV_FILENAME}.")

#         await browser.close()

# # ---------------------------
# # Run the async main function.
# # ---------------------------
# if __name__ == "__main__":
#     # On macOS, prevent sleep with the "caffeinate" command:
#     # Run: caffeinate -i python3 your_script.py
#     asyncio.run(main())
