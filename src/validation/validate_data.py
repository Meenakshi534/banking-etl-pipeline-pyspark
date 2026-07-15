import pyspark
import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\meena\AppData\Local\Programs\Python\Python310\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\meena\AppData\Local\Programs\Python\Python310\python.exe"

def check_nulls(data):
    """
    Checks for null values in the given DataFrame and prints the count of nulls for each column.
    """
    #null_counts = data.select(*[F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in data.columns]).collect()[0].asDict()
    for column in data.columns:
        null_count = data.filter(data[column].isNull()).count()
        print(f"Column '{column}' has {null_count} null values.")


def check_duplicates(data):
    """
    Checks for duplicate rows in the given DataFrame and prints the count of duplicates.
    """
    duplicate_count = data.groupBy(data.columns).count().filter("count > 1").count()
    print(f"DataFrame has {duplicate_count} duplicate rows.")

def check_orphans(data):
    orphan_count = data.filter(data['customer_id'].isNull()).count()
    print(f"DataFrame has {orphan_count} orphan rows (rows with null customer_id).")

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataIngestion").getOrCreate()

# Example usage (adjust path as needed)
df_account = spark.read.csv(r"C:/Users/meena/OneDrive/Desktop/Banking ETL Project/data/raw/accounts.csv", header=True, inferSchema=True)
df_customer=spark.read.csv("C:/Users/meena/OneDrive/Desktop/Banking ETL Project/data/raw/customers.csv", header=True, inferSchema=True)
df_transaction=spark.read.csv("C:/Users/meena/OneDrive/Desktop/Banking ETL Project/data/raw/transactions.csv", header=True, inferSchema=True)


#check_nulls(df_account)
#check_duplicates(df_account)
#check_orphans(df_account)

def check_duplicate_record(data, column_names):
    """
    checks for duplicate records in the specified column and prints the count of duplicates.
    """
    for column_name in column_names:
        duplicate_count = data.groupby(column_name).count().filter("count > 1").select(column_name).show(truncate=False)
        print(f"Column '{column_name}' has {duplicate_count} duplicate records.")

def check_orphan_accounts_without_customers(df_account, df_customer):
    """
    checks for orphan accounts without corresponding customers and prints the count of such accounts.
    """
    orphan_count = df_account.join(df_customer, df_account.customer_id == df_customer.customer_id, "left_anti").count()
    orphan_account=df_account.join(df_customer, df_account.customer_id == df_customer.customer_id, "left_anti")
    orphan_account.show(truncate=False)
    print(f"DataFrame has {orphan_count} orphan accounts without corresponding customers.")

def valid_accounts(df_account, df_customer):
    valid_account = df_account.join(df_customer, df_account.customer_id == df_customer.customer_id, "inner")
    valid_account.show(truncate=False)

#check_duplicate_record(df_customer, ['customer_id'])
#check_duplicate_record(df_account, ['account_id'])
#check_duplicate_record(df_transaction, ['transaction_id'])
#check_duplicate_record(df_transaction, ['account_id'])
#check_orphan_accounts_without_customers(df_account, df_customer)
valid_accounts(df_account, df_customer)
