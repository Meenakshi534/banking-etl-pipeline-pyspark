"""import pandas as pd

customers = [
    [1, "Rahul Sharma", "Hyderabad"],
    [2, "Priya Reddy", "Bangalore"],
    [3, "Amit Kumar", "Chennai"]
]

accounts = [
    [1001, 1, "Savings"],
    [1002, 1, "Current"],
    [1003, 2, "Savings"]
]

transactions = [
    [50001, 1001, "DEBIT", 500, "2026-06-01 10:30:00"],
    [50002, 1001, "CREDIT", 2000, "2026-06-01 11:00:00"],
    [50003, 1003, "DEBIT", 100, "2026-06-02 09:15:00"]
]

df = pd.DataFrame(
    transactions,
    columns=["transaction_id", "account_id", "transaction_type", "amount", "transaction_time"]
)

df.to_csv("C:/Users/meena/OneDrive/Desktop/Banking ETL Project/data/raw/transactions.csv", index=False)"""
from src.extract.extract_data import (
    read_customers,
    read_accounts,
    read_transactions
)

from src.transformation.transform_data import total_spent_per_customer, monthly_transaction_summary, account_activity_report, daily_balance_trend
from src.load.load_data import load_to_csv
from src.utils.logger import logger

#read
logger.info("Starting ETL Pipeline")

df_customer = read_customers()
logger.info(f"Customers loaded: {df_customer.count()}")

df_account = read_accounts()
logger.info(f"Accounts loaded: {df_account.count()}")

df_transaction = read_transactions()
logger.info(f"Transactions loaded: {df_transaction.count()}")

#transform
customer_spending=total_spent_per_customer(df_transaction, df_account)
df_monthly_summary=monthly_transaction_summary(df_transaction)
df_account_activity = account_activity_report(df_transaction)
df_running_total = daily_balance_trend(df_transaction)

#load_to_csv(customer_spending, "customer_spending_2")
#load_to_csv(df_monthly_summary, "monthly_summary")
#load_to_csv(df_account_activity, "account_activity")
load_to_csv(df_running_total, "daily_running_balance")