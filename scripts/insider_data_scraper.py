import asyncio
import requests
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path

async def scrape_insider_trades():
    purchaseUrl = 'http://openinsider.com/top-insider-purchases-of-the-month'
    saleUrl = 'http://openinsider.com/top-insider-sales-of-the-month'

    purchaseUrl_markup = requests.get(purchaseUrl).text
    saleUrl_markup = requests.get(saleUrl).text
    purchaseSoup = BeautifulSoup(purchaseUrl_markup, 'html.parser')
    saleSoup = BeautifulSoup(saleUrl_markup, 'html.parser')

    purchaseTable = purchaseSoup.find("table", class_="tinytable")
    saleTable = saleSoup.find("table", class_="tinytable")
    purchaseTBody = purchaseTable.find("tbody")
    saleTBody = saleTable.find("tbody")

    data = []

    if purchaseTBody:
        rows = purchaseTBody.find_all("tr")
        for row in rows:
            cols = row.find_all("td")
            fileDateLines = cols[1].text.split()
            dateFiled = fileDateLines[0] if len(fileDateLines) > 0 else "-"
            data.append({
                "ticker": cols[3].text.strip(),
                "company": cols[4].text.strip(),
                "transaction_type": "Purchase",
                "date_filed": dateFiled
            })

    if saleTBody:
        rows = purchaseTBody.find_all("tr")
        for row in rows:
            cols = row.find_all("td")
            fileDateLines = cols[1].text.split()
            dateFiled = fileDateLines[0] if len(fileDateLines) > 0 else "-"
            data.append({
                "ticker": cols[3].text.strip(),
                "company": cols[4].text.strip(),
                "transaction_type": "Sale",
                "date_filed": dateFiled
            })
    df = pd.DataFrame(data)
    Path("data").mkdir(exist_ok=True) # Creates data folder if not already there
    df.to_csv("data/insider_trades.csv", index=False)
    print("Scraped and saved to insider_trades.csv")

if __name__ == "__main__":
    asyncio.run(scrape_insider_trades())
