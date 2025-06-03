import os
import time
import pandas as pd
import psycopg2
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

# ✅ PostgreSQL Database Configuration
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "admin_1974",
    "host": "141.148.196.73",
    "port": "5432"
}

# ✅ File Download Setup
DOWNLOAD_DIR = os.path.join(os.getcwd(), "_cfDownloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# ✅ Chromium Browser & Chromedriver Paths
CHROMIUM_PATH = "/usr/bin/chromium-browser"
CHROMEDRIVER_PATH = "/usr/bin/chromedriver"

# ✅ Chrome Options for Selenium
options = Options()
options.binary_location = CHROMIUM_PATH
options.add_argument("--headless")  # Run in headless mode
options.add_argument("--no-sandbox")  # Helps with permissions issues
options.add_argument("--disable-dev-shm-usage")  # Avoid shared memory issues
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument(f"--user-data-dir=/tmp/chrome_profile_{int(time.time())}")  # Unique Chrome session

options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option("useAutomationExtension", False)

prefs = {
    "download.default_directory": DOWNLOAD_DIR,
    "download.prompt_for_download": False,
    "directory_upgrade": True,
    "safebrowsing.enabled": True
}
options.add_experimental_option("prefs", prefs)

# ✅ Initialize WebDriver
service = Service(CHROMEDRIVER_PATH)
driver = webdriver.Chrome(service=service, options=options)

# ✅ Remove webdriver detection
driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
    "source": """
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });
    """
})

try:
    # 🏹 Step 1: Navigate to the NSE website
    driver.get("https://www.nseindia.com/companies-listing/corporate-filings-announcements")
    time.sleep(3)

    # ✅ Accept cookies if popup appears
    try:
        cookie_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Accept')]"))
        )
        cookie_button.click()
    except:
        pass

    # 🏹 Step 2: Click download link
    download_link = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable((By.ID, "CFanncEquity-download"))
    )
    driver.execute_script("arguments[0].click();", download_link)
    print("⬇️ Downloading CSV...")

    time.sleep(15)

    # 📂 Step 3: Locate the downloaded file
    files = [f for f in os.listdir(DOWNLOAD_DIR) if f.endswith(".csv")]
    if not files:
        raise FileNotFoundError("CSV download failed.")
    latest_csv = max([os.path.join(DOWNLOAD_DIR, f) for f in files], key=os.path.getctime)
    print(f"✅ Downloaded: {latest_csv}")

    # 📊 Step 4: Read & clean the CSV
    df = pd.read_csv(latest_csv, encoding='utf-8')
    
    # Ensure column names match the expected PostgreSQL schema
    df.columns = df.columns.str.upper().str.strip().str.replace("/", " AND ", regex=False)

    df.rename(columns={
        "SYMBOL": "symbol",
        "COMPANY NAME": "company_name",
        "SUBJECT": "subject",
        "DETAILS": "details",
        "BROADCAST DATE AND TIME": "broadcast_datetime",
        "RECEIPT": "receipt",
        "DISSEMINATION": "dissemination",
        "DIFFERENCE": "difference",
        "ATTACHMENT": "attachment"
    }, inplace=True)

    df.drop(columns=["FILE SIZE"], inplace=True)
    required_cols = {"symbol", "company_name", "subject", "details", 
                     "broadcast_datetime", "receipt", "dissemination", 
                     "difference", "attachment"}

    if not required_cols.issubset(df.columns):
        raise ValueError(f"CSV missing required columns. Found: {df.columns.tolist()}")

    # Convert broadcast datetime to correct format
    df["broadcast_datetime"] = pd.to_datetime(df["broadcast_datetime"])

    # 🧹 Clean data by replacing missing values
    df = df.fillna("").replace("-", None)

    # 📌 Step 5: Insert data into PostgreSQL
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    query = """
        INSERT INTO corporate_announcements (
            symbol, company_name, subject, details,
            broadcast_datetime, receipt, dissemination, difference, attachment
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, broadcast_datetime) DO UPDATE SET
            company_name = EXCLUDED.company_name,
            subject = EXCLUDED.subject,
            details = EXCLUDED.details,
            receipt = EXCLUDED.receipt,
            dissemination = EXCLUDED.dissemination,
            difference = EXCLUDED.difference,
            attachment = EXCLUDED.attachment;
    """

    cursor.executemany(query, [tuple(row) for _, row in df.iterrows()])
    conn.commit()

    print("✅ All data inserted/updated successfully!")

except Exception as e:
    print(f"❌ Error: {e}")

finally:
    if 'driver' in locals():
        driver.quit()
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
        print("🔌 Database connection closed.")

