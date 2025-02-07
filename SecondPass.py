#!/usr/bin/env python3
import asyncio
import argparse
import csv
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# Constants and settings
FIELDNAMES = ["team", "state", "detail_url", "club_name", "club_website"]
PAGE_LOAD_TIMEOUT = 60000  # 60 seconds
RETRIES = 5
RETRY_DELAY = 5
CONCURRENCY_LIMIT = 10  # Lower concurrency for increased accuracy

# ---------------------------
# CSV Functions
# ---------------------------
def read_input_csv(input_file):
    rows = []
    with open(input_file, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, fieldnames=FIELDNAMES)
        # Skip header row if present
        header = next(reader)
        # Check if header matches (optional)
        for row in reader:
            rows.append(row)
    return rows

def write_output_csv(output_file, rows):
    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

# ---------------------------
# Helper function: safe_get with retries and networkidle wait
# ---------------------------
async def safe_get(page, url, retries=RETRIES, delay=RETRY_DELAY):
    for attempt in range(retries):
        try:
            print(f"Loading URL: {url} (attempt {attempt+1})")
            await page.goto(url, timeout=PAGE_LOAD_TIMEOUT)
            # Wait for the network to be idle to ensure page loads completely
            await page.wait_for_load_state("networkidle", timeout=PAGE_LOAD_TIMEOUT)
            print(f"Successfully loaded: {url}")
            return True
        except Exception as e:
            print(f"Attempt {attempt+1} to load {url} failed: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(delay)
            else:
                print("Maximum retries reached for URL:", url)
                return False

# ---------------------------
# Detail Extraction Function
# ---------------------------
async def extract_missing_fields(page, row):
    """
    If club_name is missing, scrape it.
    If club_website is missing (but club_name is present or after scraping club_name), scrape it.
    """
    # If club_name is missing, wait for the detail container and extract club name.
    club_name = row.get("club_name", "").strip()
    club_website = row.get("club_website", "").strip()
    
    # First, check if we need to update club name
    if not club_name:
        try:
            # Wait for the club info container to load
            await page.wait_for_selector("//div[span[text()='Club Information']]", timeout=PAGE_LOAD_TIMEOUT)
            club_name_elem = await page.wait_for_selector("//span[text()='Club Name']/following-sibling::span[1]", timeout=5000)
            if club_name_elem:
                club_name = (await club_name_elem.inner_text()).strip()
                print(f"Scraped club name: {club_name}")
        except Exception as e:
            print("Could not extract club name:", e)

    # Next, if club_website is missing, scrape it.
    if not club_website:
        try:
            club_website_elem = await page.wait_for_selector("//span[text()='Website']/following-sibling::span//a", timeout=5000)
            if club_website_elem:
                club_website = await club_website_elem.get_attribute("href")
                print(f"Scraped club website: {club_website}")
        except Exception as e:
            print("Could not extract club website:", e)
    
    return club_name, club_website

# ---------------------------
# Process a Single Row
# ---------------------------
async def process_row(row, browser):
    """
    Processes one CSV row by opening the detail_url and scraping any missing fields.
    Returns the updated row.
    """
    # If both fields are present, no need to process.
    if row.get("club_name", "").strip() and row.get("club_website", "").strip():
        print(f"Skipping {row['team']} as both club name and website are present.")
        return row

    context = await browser.new_context()
    page = await context.new_page()
    url = row.get("detail_url")
    if not url:
        print(f"No detail URL for team: {row.get('team')}")
        await context.close()
        return row

    loaded = await safe_get(page, url)
    if loaded:
        club_name, club_website = await extract_missing_fields(page, row)
        # Update the row only if we scraped new info (do not overwrite if already exists)
        if not row.get("club_name", "").strip() and club_name:
            row["club_name"] = club_name
        if not row.get("club_website", "").strip() and club_website:
            row["club_website"] = club_website
    else:
        print(f"Failed to load page for team: {row.get('team')}")
    await context.close()
    return row

# ---------------------------
# Process All Rows with Concurrency and Checkpointing
# ---------------------------
async def process_all_rows(rows):
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        
        async def process_with_semaphore(row):
            async with semaphore:
                return await process_row(row, browser)
        
        total = len(rows)
        updated_rows = []
        # Process rows sequentially in batches (for checkpointing)
        batch_size = BATCH_SIZE if BATCH_SIZE > 0 else total
        for i in range(0, total, batch_size):
            batch = rows[i:i+batch_size]
            print(f"Processing batch {i//batch_size+1} (rows {i+1} to {i+len(batch)})...")
            batch_results = await asyncio.gather(*(process_with_semaphore(row) for row in batch))
            updated_rows.extend(batch_results)
            # Optionally, write checkpoint after each batch
            write_output_csv("SecondPassOutput_checkpoint.csv", updated_rows)
            print(f"Checkpoint: Processed {len(batch_results)} rows.")
        await browser.close()
    return updated_rows

# ---------------------------
# Command-Line Parsing
# ---------------------------
def parse_arguments():
    parser = argparse.ArgumentParser(description="Second Pass: Fill in missing club info")
    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Input CSV file with columns: team, state, detail_url, club_name, club_website"
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Output CSV file to write updated data"
    )
    return parser.parse_args()

# ---------------------------
# Main Function
# ---------------------------
async def main():
    args = parse_arguments()
    rows = read_input_csv(args.input)
    print(f"Read {len(rows)} rows from {args.input}")
    updated_rows = await process_all_rows(rows)
    write_output_csv(args.output, updated_rows)
    print(f"Second pass complete. Updated data written to {args.output}")

if __name__ == "__main__":
    asyncio.run(main())
