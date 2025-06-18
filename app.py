import requests, gzip, io, os, sqlite3
from datetime import datetime
import xml.etree.ElementTree as ET
import re

HEADERS = {'User-Agent': 'Abhay Rathour abhay@example.com'}

def download_master_index(year, quarter):
    url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.gz"
    print(f"Downloading {url}")
    r = requests.get(url, headers=HEADERS)
    with gzip.open(io.BytesIO(r.content), 'rt') as f:
        lines = f.readlines()
    return [line for line in lines if '|13F-HR|' in line]

def parse_master_line(line):
    parts = line.strip().split('|')
    if len(parts) == 5:
        cik, name, form, date, path = parts
        return {
            "cik": cik,
            "name": name,
            "date": date,
            "url": f"https://www.sec.gov/Archives/{path}"
        }

# def find_info_table_url(filing_url):
#     html = requests.get(filing_url, headers=HEADERS).text
#     metadata = html.splitlines()
#     print(len(metadata), "lines in metadata")
#     print(metadata)
#     print(f"Searching for info table in {filing_url}")
#     base_url = filing_url.rsplit('/', 1)[0]
#     print("base_url", base_url)
#     for line in html.splitlines():
#         if 'INFORMATION TABLE' in line and ('.xml' in line or '.txt' in line):
#             filename = line.split('href="')[1].split('"')[0]
#             return f"{base_url}/{filename}"
#     return None



def find_info_table_url(filing_txt_url):
    """
    Given a filing .txt URL (e.g. https://www.sec.gov/Archives/edgar/data/.../0001000097-24-000004.txt),
    download the .txt file, extract the INFORMATION TABLE FILENAME, and construct the XML URL.
    """
    headers = {"User-Agent": "Your Name your@email.com"}
    response = requests.get(filing_txt_url, headers=headers)
    
    if response.status_code != 200:
        raise Exception(f"Failed to fetch filing text file: {filing_txt_url}")
    
    text = response.text
    
    # Find the <TYPE>INFORMATION TABLE section
    match = re.search(r'<TYPE>\s*INFORMATION TABLE\s*<SEQUENCE>\d+\s*<FILENAME>([^\s<]+)', text, re.IGNORECASE)
    if not match:
        raise Exception("Could not find INFORMATION TABLE filename in .txt content")
    
    xml_filename = match.group(1).strip()
    
    # Construct base URL by removing .txt and keeping the folder path
    base_url = filing_txt_url.rsplit("/", 1)[0]
    print("filing_txt_url", filing_txt_url)
    return f"{base_url}/{xml_filename}"


def parse_holdings_from_xml(xml_url):
    r = requests.get(xml_url, headers=HEADERS)
    root = ET.fromstring(r.content)
    rows = []
    for info in root.findall('.//infoTable'):
        rows.append({
            "name": info.findtext('nameOfIssuer'),
            "cusip": info.findtext('cusip'),
            "value": int(info.findtext('value') or 0),
            "shares": int(info.findtext('sshPrnamt') or 0),
            "type": info.findtext('sshPrnamtType'),
        })
    return rows

def save_to_sqlite(filing, holdings, db_name="13f_filings.db"):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS filings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cik TEXT,
        name TEXT,
        date TEXT,
        xml_url TEXT
    )''')
    cur.execute('''CREATE TABLE IF NOT EXISTS holdings (
        filing_id INTEGER,
        name TEXT,
        cusip TEXT,
        value INTEGER,
        shares INTEGER,
        type TEXT,
        FOREIGN KEY(filing_id) REFERENCES filings(id)
    )''')
    
    cur.execute("INSERT INTO filings (cik, name, date, xml_url) VALUES (?, ?, ?, ?)",
                (filing['cik'], filing['name'], filing['date'], filing['xml_url']))
    filing_id = cur.lastrowid
    for h in holdings:
        cur.execute("INSERT INTO holdings (filing_id, name, cusip, value, shares, type) VALUES (?, ?, ?, ?, ?, ?)",
                    (filing_id, h['name'], h['cusip'], h['value'], h['shares'], h['type']))
    conn.commit()
    conn.close()

def run_pipeline(year=2024, quarter=1, limit=2):
    lines = download_master_index(year, quarter)
    print(f"Found {len(lines)} 13F-HR filings.")
    count = 5
    for line in lines[:2]:
        # if count <= 10:

        filing = parse_master_line(line)  # For simplicity, just take the first filing
        # if not filing:
        #     continue
        print("Processing filing:", filing["name"])
        xml_url = find_info_table_url(filing["url"])
        print("XML URL:", xml_url)
        # if not xml_url:
        #     continue
        try:
            holdings = parse_holdings_from_xml(xml_url)
            filing["xml_url"] = xml_url
            save_to_sqlite(filing, holdings)
            print(f"Saved {len(holdings)} holdings from {filing['name']}")
            count += 1
        except Exception as e:
            print(f"Failed: {e}")
    # if count >= limit:
    #     break

if __name__ == "__main__":
    run_pipeline(2024, 1)
