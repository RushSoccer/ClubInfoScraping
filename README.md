# SecondPass.py – Incremental Club Info Scraper (Second Pass)

This Python script is designed to update an existing CSV file of club information by filling in missing club names and websites. It does this by re-visiting the detail URLs provided in the input CSV and scraping the missing fields. The script also uses checkpointing so that if it stops unexpectedly (due to timeouts or other issues), you can resume processing from where it left off.

For optimal performance and to avoid long run times on underpowered hardware, it is recommended to run this script in a virtual machine (VM) with sufficient compute power.

## Features

- **Incremental Processing:**  
  The script reads an input CSV (e.g. a file from a previous scrape) and processes only the rows that have missing club name or website data. It uses a checkpoint file (`SecondPassOutput_checkpoint.csv`) to save progress after every batch.

- **Checkpointing:**  
  After every batch of 500 rows (default), the updated rows are written to the checkpoint file. This lets you resume the process without reprocessing already updated rows.

- **Concurrency Control:**  
  A concurrency limit (default 10) is used to control how many detail pages are processed at once. This trade-off improves accuracy (by lowering load) while still allowing some level of parallelism.

- **Robust URL Loading:**  
  The script uses retries (default 5 attempts, with a 5-second delay between attempts) and waits for the page's network idle state before proceeding. This improves the chance that the page is fully loaded before extraction.

## How to Use This Script

### Prerequisites

- **Python 3.10+** is required.
- The script uses [Playwright](https://playwright.dev/python/docs/intro) for asynchronous browser control. Make sure you have installed the necessary packages and dependencies:
  - Install packages with pip:  
    ```bash
    pip3 install playwright numpy
    ```
  - Install browser dependencies (this script assumes you are running on a Linux-based VM):  
    ```bash
    sudo playwright install-deps
    ```
  - Install the browsers (if not already done):  
    ```bash
    playwright install chromium
    ```
  
  For optimal results, run these commands on a virtual machine because the scraping process may be resource-intensive and take a long time on less powerful machines.

### Files

- **Input CSV:**  
  The script expects an input CSV file (e.g., `ClubInfo-SecondPass.csv`) that contains at least the following columns:  
  `team, state, detail_url, club_name, club_website`  
  Rows where the club name or website is missing will be processed.

- **Checkpoint File:**  
  The script will write checkpoint progress to `SecondPassOutput_checkpoint.csv` (in the current working directory) after each batch of 500 rows.

- **Output CSV:**  
  The final updated data (including both unchanged and updated rows) is written to the output CSV file you specify (e.g., `SecondPassOutput.csv`).

### How to Execute

1. **Clone or Update Your Repository:**
   Make sure you have the latest version of the code:
   ```bash
   cd ~/ClubInfoScraping
   git pull origin master
    ```

2.  **Run the Script in a Persistent Terminal Session:**  
  It is highly recommended to use a terminal multiplexer such as `tmux` so that your process continues running even if your SSH session disconnects:

3. **Execute the Script:**  
Run the script by specifying the input and output CSV files:

4. 
- **To Resume if the Process Fails:**  
The script automatically writes checkpoints. If it fails before completing, simply re-run the same command. The script will load the checkpoint file (`SecondPassOutput_checkpoint.csv`) and only process rows that are missing data.


