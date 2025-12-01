import asyncio
import sys
from playwright.async_api import async_playwright
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path
import time

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

async def clean(cell):  
    text = await cell.text_content()
    return " ".join(text.split())

async def scrape_top_holds():
    url = "https://whalewisdom.com"
    fund_links = []
    data = []

    async with async_playwright() as p: #starts playwright context
        browser = await p.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox', # Disables chrome's sandbox environment, which websites can flag as potential bots
                '--disable-bots', # Stop's chrome from flagging itself as a bot to other websites
                '--disable-web-security', #Disables security restrictions with these headless browsers, some websites pick up and flag restrictions
                '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' # Sets up a realistic user agent string
            ]
        )
        page = await browser.new_page()

        await page.goto(url, wait_until="networkidle")

        #gets HTML content
        html_content = await page.content()
        soup = BeautifulSoup(html_content, 'html.parser')
        await page.close()

        div_wrapper = soup.find('div', class_='v-table__wrapper')
        if div_wrapper:
            tbody = div_wrapper.find('tbody')
            for link in tbody.find_all('a', href=True):
                href = link.get('href')
                if(href and href.startswith('/filer/')):
                    fullUrl = url + href
                    fund_links.append(fullUrl)

        print(len(fund_links))
        fund_links = fund_links[:25]
        
        for i in range(len(fund_links)):
            fundPage = await browser.new_page()
            # navigate the new fundPage to the fund URL
            await fundPage.goto(fund_links[i])

            # scroll down to bottom of page so tables load correctly
            await fundPage.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            
            # scroll back to top
            await fundPage.evaluate("window.scrollTo(0, 0)")
            
            # scroll down again to ensure tables are fully loaded
            await fundPage.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await fundPage.wait_for_timeout(5000)  # delay so JS can run

            try:
                await fundPage.wait_for_selector("table", timeout=15000)
            except Exception:
                print("No table appeared on", fund_links[i])
                await fundPage.close()
                continue

            # get the HTML from the *page*, not the response
            fundNameElement = await fundPage.query_selector("h1")
            if not fundNameElement:
                print("Could not find fund name for", fund_links[i])
                await fundPage.close()
                continue
                
            fundNameText = await fundNameElement.text_content()
            fundName = fundNameText.strip()

            print("Scraping " + fundName + " data")
            
            dateElem = await fundPage.query_selector("div.v-col.v-col-auto.pt-2 > p")
            date = ""
            if dateElem:
                date = (await dateElem.text_content()).strip()
            
            tableList = await fundPage.query_selector_all("table")

            if len(tableList) < 2:
                print("Not enough tables found for", fundName)
                await fundPage.close()
                continue

            buyTable = tableList[0]
            sellTable = tableList[1]
            buyRows = await buyTable.query_selector_all("tbody tr")
            sellRows = await sellTable.query_selector_all("tbody tr")

            if(buyRows):
                for row in buyRows:
                    if(await row.query_selector("td") and len(tableList) == 4):
                        tickerElement = await row.query_selector('td strong')
                        if tickerElement:
                            tickerElement = await row.query_selector("td strong")
                            tickerText = await tickerElement.text_content()
                            ticker = tickerText.strip()
                        descriptionElement = await row.query_selector("td a")
                        if descriptionElement:
                            cleanedElem = await clean(descriptionElement)
                            description = cleanedElem.strip()
                            # Skip options trades (CALL or PUT)
                            if "(CALL)" in description or "(PUT)" in description:
                                continue
                        data.append({
                            "ticker": ticker,
                            "description": description,
                            "hedge_fund": fundName,
                            "action": "Purchase",
                            "date_updated": date
                        })
                    else:
                        ("Table does not exist")
            if(sellRows):
                for row in sellRows:
                    if(await row.query_selector("td") and len(tableList) == 4):
                        tickerElement = await row.query_selector('td strong')
                        if tickerElement:
                            tickerElement = await row.query_selector("td strong")
                            tickerText = await tickerElement.text_content()
                            ticker = tickerText.strip()
                        descriptionElement = await row.query_selector("td a")
                        if descriptionElement:
                            cleanedElem = await clean(descriptionElement)
                            description = cleanedElem.strip()
                            # Skip options trades (CALL or PUT)
                            if "(CALL)" in description or "(PUT)" in description:
                                continue
                        data.append({
                            "ticker": ticker,
                            "description": description,
                            "hedge_fund": fundName,
                            "action": "Sale",
                            "date_updated": date
                        })
                    else:
                        ("Table does not exist")
            await fundPage.close()

        df = pd.DataFrame(data)
        Path("data").mkdir(exist_ok=True) # Creates data folder if not already there
        df.to_csv("data/hedge_fund_trades.csv", index=False)
        print("Scraped and saved to hedge_fund-trades.csv")

if __name__ == "__main__":
    asyncio.run(scrape_top_holds())