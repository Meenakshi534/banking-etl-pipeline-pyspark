import pyspark
import os


os.environ["PYSPARK_PYTHON"] = r"C:\Users\meena\AppData\Local\Programs\Python\Python310\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\meena\AppData\Local\Programs\Python\Python310\python.exe"


os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataIngestion").getOrCreate()
from src.utils.logger import logger

# Example usage (adjust path as needed)
df_account = spark.read.csv(r"C:/Users/meena/OneDrive/Desktop/Banking ETL Project/data/raw/accounts.csv", header=True, inferSchema=True)
df_customer=spark.read.csv("C:/Users/meena/OneDrive/Desktop/Banking ETL Project/data/raw/customers.csv", header=True, inferSchema=True)
df_transaction=spark.read.csv("C:/Users/meena/OneDrive/Desktop/Banking ETL Project/data/raw/transactions.csv", header=True, inferSchema=True)

print(df_account.columns)
print(df_customer.columns)
print(df_transaction.columns)


def remove_duplicate_rows(data, subset_columns):
    cleaned_data = data.dropDuplicates(subset=subset_columns)
    cleaned_data.toPandas().to_csv(r"C:/Users/meena/OneDrive/Desktop/Banking ETL Project/data/processed/transactions.csv", index=False)
    logger.info(f"Removed duplicate rows")
    return cleaned_data

def remove_orphan_accounts(df_account, df_customer):
    cleaned_data = df_account.join(df_customer, on="customer_id", how="inner").select("account_id", "customer_id", "account_type")
    cleaned_data.toPandas().to_csv(r"C:/Users/meena/OneDrive/Desktop/Banking ETL Project/data/processed/accounts.csv", index=False)
    print(cleaned_data)

def remove_orphan_transactions(df_transactions, df_account):
    cleaned_data = df_transactions.join(df_account, on="account_id", how="inner").select("transaction_id", "account_id", "transaction_type", "amount", "timestamp")
    cleaned_data.toPandas().to_csv(r"C:/Users/meena/OneDrive/Desktop/Banking ETL Project/data/processed/transactions.csv", index=False)
    return cleaned_data

from pyspark.sql.functions import col, count, when
#df_transaction.filter(col("amount") <=0).show()
#df_transaction.filter(col("transaction_type").isin("DEBIT", "CREDIT") == False).show() #find invalid transaction types
#df_account.filter(col("account_type").isin("Savings", "Current") == False).show() #find invalid account types

#remove_duplicate_rows(df_customer, ['customer_id']).show(truncate=False)
#remove_duplicate_rows(df_account, ['account_id']).show(truncate=False)
#remove_duplicate_rows(df_transaction, ['transaction_id']).show(truncate=False)

#remove_orphan_accounts(df_account, df_customer)
remove_orphan_transactions(df_transaction, df_account).show(truncate=False)
