import asyncio
from playwright.async_api import async_playwright # Controls the browser
import pandas as pd # Handles data
from pathlib import Path # Handles file paths
import sys

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
from playwright.sync_api import sync_playwright

async def clean(cell):
    text = await cell.text_content()
    return " ".join(text.split())

async def scrape_congress_trades():
    # Launch a Playwright context
    async with async_playwright() as p:
        # Start a headless browser (runs without showing a window/silent)
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # Navigate to the QuiverQuant congress trading data table
        await page.goto("https://www.quiverquant.com/congresstrading")

        # Wait for main table to load
        await page.wait_for_selector("table")

        # Grab only the first table (recent trades)
        table = await page.query_selector("table")
        rows = await table.query_selector_all("tbody tr")

        data = []

        for row in rows:
            cols = await row.query_selector_all("td")
            if len(cols) >= 6:
                # Parse the first column (Ticker and Description in same cell)
                text0 = await cols[0].text_content()
                cell_0_lines = text0.splitlines()
                cell_0_lines = [line.strip() for line in cell_0_lines if line.strip()]

                ticker = cell_0_lines[0] if len(cell_0_lines) > 0 else "-"
                description = cell_0_lines[1] if len(cell_0_lines) > 1 else "-"

                text1 = await cols[1].text_content()
                cell_1_lines = text1.splitlines()
                cell_1_lines = [line.strip() for line in cell_1_lines if line.strip()]

                transaction_type = cell_1_lines[0] if len(cell_1_lines) > 0 else "-"
                amount = cell_1_lines[1] if len(cell_1_lines) > 1 else "-"

                # Clean the other columns
                politician = await clean(cols[2])
                #filed = clean(cols[3])
                traded = await clean(cols[4])
                returns = await clean(cols[6])

                # Add to data list
                data.append({
                    "ticker": ticker,
                    "description": description,
                    "transaction_type": transaction_type,
                    "amount": amount,
                    "politician": politician,
                    #"Filed": filed,
                    "trade_date": traded,
                    "returns": returns
                })
        await browser.close()
        
        df = pd.DataFrame(data)
        Path("data").mkdir(exist_ok=True) #Make the data folder if it doesn't already exist
        df.to_csv("data/congress_trades.csv", index=False)
        print("Scraped and saved recent trades to data/congress_trades.csv")

if __name__ == "__main__":
    asyncio.run(scrape_congress_trades())