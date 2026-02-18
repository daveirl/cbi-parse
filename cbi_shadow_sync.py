import requests
from bs4 import BeautifulSoup
import pdfplumber
import pandas as pd
import io
import re
import os
from datetime import datetime, timedelta

# Configuration
DOWNLOADS_PAGE = "https://registers.centralbank.ie/DownloadsPage.aspx"
TARGET_TEXT = "Authorised UCITS, European Communities (Undertakings for Collective Investment in Transferable Securities) Regulations 2011"
DB_FILE = "cbi_shadow_db.csv"

def standardize_date(date_str):
    for fmt in ("%d %b %Y", "%d-%b-%y", "%d %B %Y"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return date_str

def run_sync():
    # 1. Load or Initialize Shadow DB
    if os.path.exists(DB_FILE):
        shadow_df = pd.read_csv(DB_FILE)
    else:
        shadow_df = pd.DataFrame(columns=["Fund Name", "Auth_Date", "First_Seen"])

    # 2. Fetch Live PDF via PostBack
    session = requests.Session()
    res = session.get(DOWNLOADS_PAGE)
    soup = BeautifulSoup(res.text, 'html.parser')
    
    payload = {
        "__EVENTTARGET": "",
        "__EVENTARGUMENT": "",
        "__VIEWSTATE": soup.find("input", {"id": "__VIEWSTATE"})['value'],
        "__VIEWSTATEGENERATOR": soup.find("input", {"id": "__VIEWSTATEGENERATOR"})['value'],
        "__EVENTVALIDATION": soup.find("input", {"id": "__EVENTVALIDATION"})['value']
    }
    
    for link in soup.find_all('a', href=True):
        if TARGET_TEXT in link.text:
            match = re.search(r"'(.*?)'", link['href'])
            if match:
                payload["__EVENTTARGET"] = match.group(1)
                break

    pdf_res = session.post(DOWNLOADS_PAGE, data=payload)
    
    # 3. Extract and Compare
    new_funds = []
    date_pattern = re.compile(r'(\d{1,2}[- ](?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[- ]\d{2,4})')

    with pdfplumber.open(io.BytesIO(pdf_res.content)) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                for line in text.split('\n'):
                    match = date_pattern.search(line)
                    if match:
                        name = re.sub(r'\s+', ' ', line[:match.start()]).strip()
                        if name and name not in shadow_df['Fund Name'].values:
                            new_funds.append({
                                "Fund Name": name,
                                "Auth_Date": standardize_date(match.group(0).strip()),
                                "First_Seen": datetime.now().strftime("%Y-%m-%d")
                            })

    # 4. Generate Outputs
    if new_funds:
        shadow_df = pd.concat([shadow_df, pd.DataFrame(new_funds)], ignore_index=True)
    
    shadow_df = shadow_df.sort_values(by="Auth_Date", ascending=False)
    shadow_df.to_csv(DB_FILE, index=False) # Save updated master

    # Output A: Full Database
    shadow_df.to_excel("CBI_Full_Database.xlsx", index=False)

    # Output B: New Funds (Added in this run)
    if new_funds:
        pd.DataFrame(new_funds).to_excel("CBI_New_Weekly_Funds.xlsx", index=False)

    # Output C: All ETFs
    etf_df = shadow_df[shadow_df['Fund Name'].str.contains("ETF", case=False, na=False)]
    etf_df.to_excel("CBI_All_ETFs_List.xlsx", index=False)

if __name__ == "__main__":
    run_sync()
