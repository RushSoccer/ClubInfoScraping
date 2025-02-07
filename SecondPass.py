#!/usr/bin/env python3
import asyncio
import argparse
import csv
import os
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# Constants
FIELDNAMES = ["team", "state", "detail_url", "club_name", "club_website"]
PAGE_LOAD_TIMEOUT = 60000  # 60 seconds
CONCURRENCY_LIMIT = 10     # Lower concurrency for accuracy
BATCH_SIZE = 500           # Checkpoint after processing 500 rows
RETRIES = 5
RETRY_DELAY = 5

# ---------------------------
# CSV Helper Functions
# ---------------------------
def read_csv_file(filename):
    rows = []
    with open(filename, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            rows.append(row)
    return rows

def write_csv_file(filename, rows):
    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

def load_checkpoint(checkpoint_file):
    checkpoint_data = {}
    if os.path.exists(checkpoint_file):
        rows = read_csv_file(checkpoint_file)
        for row in rows:
            detail_url = row.get("detail_url", "").strip()
            if detail_url:
                checkpoint_data[detail_url] = row
    return checkpoint_data

def merge_results(input_rows, checkpoint_data, new_results):
    # Build a lookup dictionary from the new results (using detail_url as key)
    new_results_dict = {row.get("detail_url", "").strip(): row for row in new_results if row.get("detail_url")}
    final_results = []
    for row in input_rows:
        detail_url = row.get("detail_url", "").strip()
        if detail_url in checkpoint_data:
            final_results.append(checkpoint_data[detail_url])
        elif detail_url in new_results_dict:
            final_results.append(new_results_dict[detail_url])
        else:
            final_results.append(row)
    return final_results

# ---------------------------
# Helper: safe_get with retries and waiting for network idle
# ---------------------------
async def safe_get(page, url, retries=RETRIES, delay=RETRY_DELAY):
    for attempt in range(retries):
        try:
            print(f"Loading URL: {url} (attempt {attempt+1})")
            await page.goto(url, timeout=PAGE_LOAD_TIMEOUT)
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
    # Retrieve current values
    club_name = row.get("club_name", "").strip()
    club_website = row.get("club_website", "").strip()
    
    if not club_name:
        try:
            await page.wait_for_selector("//div[span[text()='Club Information']]", timeout=PAGE_LOAD_TIMEOUT)
            club_name_elem = await page.wait_for_selector("//span[text()='Club Name']/following-sibling::span[1]", timeout=5000)
            if club_name_elem:
                club_name = (await club_name_elem.inner_text()).strip()
                print(f"Scraped club name: {club_name}")
        except Exception as e:
            print("Could not extract club name:", e)
    
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
    # If both fields are present, skip processing this row.
    if row.get("club_name", "").strip() and row.get("club_website", "").strip():
        print(f"Skipping {row['team']} as both club name and website are present.")
        return row

    context = await browser.new_context()
    page = await context.new_page()
    url = row.get("detail_url", "").strip()
    if not url:
        print("No detail URL for team:", row.get("team"))
        await context.close()
        return row

    loaded = await safe_get(page, url)
    if loaded:
        scraped_name, scraped_website = await extract_missing_fields(page, row)
        if not row.get("club_name", "").strip() and scraped_name:
            row["club_name"] = scraped_name
        if not row.get("club_website", "").strip() and scraped_website:
            row["club_website"] = scraped_website
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
        for i in range(0, total, BATCH_SIZE):
            batch = rows[i:i+BATCH_SIZE]
            print(f"\nProcessing batch {i // BATCH_SIZE + 1} (rows {i+1} to {i+len(batch)})...")
            try:
                batch_results = await asyncio.gather(*(process_with_semaphore(row) for row in batch))
            except Exception as e:
                print("Exception during batch processing:", e)
                batch_results = []
            updated_rows.extend(batch_results)
            # Write checkpoint after each batch
            write_csv_file("SecondPassOutput_checkpoint.csv", updated_rows)
            print(f"Checkpoint: Processed {len(batch_results)} rows.")
        await browser.close()
    return updated_rows

# ---------------------------
# Command-Line Argument Parsing
# ---------------------------
def parse_arguments():
    parser = argparse.ArgumentParser(description="Second Pass: Fill in missing club info")
    parser.add_argument("--input", type=str, required=True, help="Input CSV file")
    parser.add_argument("--output", type=str, required=True, help="Output CSV file for updated data")
    return parser.parse_args()

# ---------------------------
# Main Function
# ---------------------------
async def main():
    args = parse_arguments()
    input_rows = read_csv_file(args.input)
    print(f"Read {len(input_rows)} rows from {args.input}")
    
    checkpoint_file = "SecondPassOutput_checkpoint.csv"
    checkpoint_data = load_checkpoint(checkpoint_file)
    
    # Filter rows that are missing either club_name or club_website.
    rows_to_process = [row for row in input_rows if not (row.get("club_name", "").strip() and row.get("club_website", "").strip())]
    print(f"{len(rows_to_process)} rows remain to be processed after checkpoint filtering.")
    
    new_results = await process_all_rows(rows_to_process)
    
    # Merge checkpoint data and newly processed results with the original input rows.
    final_results = merge_results(input_rows, checkpoint_data, new_results)
    write_csv_file(args.output, final_results)
    print(f"Second pass complete. Updated data written to {args.output}")

if __name__ == "__main__":
    asyncio.run(main())
