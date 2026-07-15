import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RAW_DATA_PATH = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DATA_PATH = os.path.join(BASE_DIR, "data", "processed")
CURATED_DATA_PATH = os.path.join(BASE_DIR, "data", "curated")
LOGS_PATH = os.path.join(BASE_DIR, "logs")

CUSTOMERS_FILE = os.path.join(RAW_DATA_PATH, "customers.csv")
ACCOUNTS_FILE = os.path.join(RAW_DATA_PATH, "accounts.csv")
TRANSACTIONS_FILE = os.path.join(RAW_DATA_PATH, "transactions.csv")

PROCESSED_CUSTOMERS_FILE = os.path.join(PROCESSED_DATA_PATH, "customers.csv")
PROCESSED_ACCOUNTS_FILE = os.path.join(PROCESSED_DATA_PATH, "accounts.csv")
PROCESSED_TRANSACTIONS_FILE = os.path.join(PROCESSED_DATA_PATH, "transactions.csv")


