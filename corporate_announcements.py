import os
import time
import tempfile
import pandas as pd
import psycopg2
import chromedriver_autoinstaller
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

# ✅ DB Config
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

# ✅ WebDriver Manager Setup
os.environ['WDM_LOCAL'] = '1'
os.environ['WDM_CACHE_DIR'] = os.path.join(os.getcwd(), '.wdm_cache')

from webdriver_manager.chrome import ChromeDriverManager

chromedriver_autoinstaller.install()

# ✅ Chrome Options
options = Options()
options.binary_location = "/usr/bin/chromium-browser"
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--remote-debugging-port=9222")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option("useAutomationExtension", False)
user_data_dir = tempfile.mkdtemp()
options.add_argument(f"--user-data-dir={user_data_dir}")
prefs = {
    "download.default_directory": DOWNLOAD_DIR,
    "download.prompt_for_download": False,
    "directory_upgrade": True,
    "safebrowsing.enabled": True
}
options.add_experimental_option("prefs", prefs)

# ✅ Initialize WebDriver
CHROMEDRIVER_PATH = "/usr/bin/chromedriver"
service = Service("/usr/lib/chromium-browser/chromedriver")
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
    # Step 1: Navigate to URL
    driver.get("https://www.nseindia.com/companies-listing/corporate-filings-announcements")
    time.sleep(3)

    # Accept cookies if popup
    try:
        cookie_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Accept')]"))
        )
        cookie_button.click()
    except:
        pass

    # Step 2: Click download link
    download_link = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable((By.ID, "CFanncEquity-download"))
    )
    driver.execute_script("arguments[0].click();", download_link)
    print("⬇️ Downloading CSV...")

    time.sleep(15)

    # 📂 Step 3: Locate downloaded file
    files = [f for f in os.listdir(DOWNLOAD_DIR) if f.endswith(".csv")]
    if not files:
        raise FileNotFoundError("❌ CSV download failed.")
    latest_csv = max([os.path.join(DOWNLOAD_DIR, f) for f in files], key=os.path.getctime)
    print(f"✅ Downloaded: {latest_csv}")

    # 📊 Step 4: Read CSV with cleaned columns
    df = pd.read_csv(latest_csv, encoding='utf-8')
    df.columns = df.columns.str.upper().str.strip().str.replace("/", " AND ", regex=False)

    df.rename(columns={
        "COMPANY NAME": "COMPANY_NAME",
        "BROADCAST DATE AND TIME": "BROADCAST_DATETIME"
    }, inplace=True)

    required_cols = {
        "SYMBOL", "COMPANY_NAME", "SUBJECT", "DETAILS",
        "BROADCAST_DATETIME", "RECEIPT", "DISSEMINATION", "DIFFERENCE", "ATTACHMENT"
    }

    if not required_cols.issubset(df.columns):
        raise ValueError(f"❌ CSV missing required columns. Found: {df.columns.tolist()}")

    # 🧹 Clean data
    df = df.fillna("").replace("-", None)

    # ✅ Step 5: Push to DB
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO corporate_announcements (
                SYMBOL, COMPANY_NAME, SUBJECT, DETAILS,
                BROADCAST_DATETIME, RECEIPT, DISSEMINATION, DIFFERENCE, ATTACHMENT
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (SYMBOL, BROADCAST_DATETIME) DO UPDATE SET
                COMPANY_NAME = EXCLUDED.COMPANY_NAME,
                SUBJECT = EXCLUDED.SUBJECT,
                DETAILS = EXCLUDED.DETAILS,
                RECEIPT = EXCLUDED.RECEIPT,
                DISSEMINATION = EXCLUDED.DISSEMINATION,
                DIFFERENCE = EXCLUDED.DIFFERENCE,
                ATTACHMENT = EXCLUDED.ATTACHMENT;
        """, (
            row["SYMBOL"], row["COMPANY_NAME"], row["SUBJECT"], row["DETAILS"],
            row["BROADCAST_DATETIME"], row["RECEIPT"], row["DISSEMINATION"],
            row["DIFFERENCE"], row["ATTACHMENT"]
        ))

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

