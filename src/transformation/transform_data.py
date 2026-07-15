
import pyspark
import os
from config.config import PROCESSED_CUSTOMERS_FILE, PROCESSED_ACCOUNTS_FILE, PROCESSED_TRANSACTIONS_FILE


os.environ["PYSPARK_PYTHON"] = r"C:\Users\meena\AppData\Local\Programs\Python\Python310\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\meena\AppData\Local\Programs\Python\Python310\python.exe"


os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
spark = SparkSession.builder.appName("DataIngestion").getOrCreate()
from src.utils.logger import logger


df_account = spark.read.csv(PROCESSED_CUSTOMERS_FILE, header=True, inferSchema=True)
df_customer = spark.read.csv(PROCESSED_ACCOUNTS_FILE, header=True, inferSchema=True)
df_transaction = spark.read.csv(PROCESSED_TRANSACTIONS_FILE, header=True, inferSchema=True)

print(df_account.columns)
print(df_customer.columns)
print(df_transaction.columns)

from pyspark.sql.functions import col, count, when, sum, year, month
def total_spent_per_customer(df_transaction, df_account):
    logger.info("Calculating total spending per customer")
    # Join transactions with accounts to get customer_id
    df_joined = df_transaction.join(df_account, on="account_id", how="inner")

    # Filter for DEBIT transactions and group by customer_id to calculate total spent
    df_total_spent = df_joined.filter(df_joined.transaction_type == "DEBIT")
    print(df_total_spent.columns)
    df_total_spent=df_total_spent.groupBy("customer_id").agg(sum("amount").alias("Total_spent"),count("transaction_id").alias("total_transactions"))
    df_total_spent = df_total_spent.join(df_customer, on="customer_id", how="inner")
    logger.info("Customer spending calculation completed")
    return df_total_spent
    #df_total_spent.toPandas().to_csv(r"C:/Users/meena/OneDrive/Desktop/Banking ETL Project/data/curated/customer_spending.csv", index=False)
    #return df_total_spent.select("transaction_type", "amount").show()

def monthly_transaction_summary(df_transaction):
    df_with_date = (
        df_transaction
        .withColumn("year", year("timestamp"))
        .withColumn("month", month("timestamp"))
    )
    df_with_date=df_with_date.filter(df_with_date.transaction_type == 'DEBIT').orderBy("timestamp")
    df_monthly_transactions=df_with_date.groupBy("year", "month").agg(sum("amount").alias("Total_spent"), count("transaction_id").alias("total_transactions"))
    return df_monthly_transactions
    #df_monthly_transactions.toPandas().to_csv(r"C:/Users/meena/OneDrive/Desktop/Banking ETL Project/data/curated/monthly_summary.csv", index=False)


def account_activity_report(df_transaction):
    df_more_txn = df_transaction.groupBy("account_id").agg(sum("amount").alias("Total_amount"), count("account_id").alias("total_transactions"))
    df_more_txn = df_more_txn.sort("total_transactions")
    return df_more_txn
    #df_more_txn.toPandas().to_csv(r"C:/Users/meena/OneDrive/Desktop/Banking ETL Project/data/curated/account_activity.csv", index=False)

def customer_seggregation(df_transaction, df_account):
    df_joined=df_transaction.join(df_account, on="account_id", how="inner")
    print(df_joined.columns)
    df_debit_txns=df_joined.filter(df_joined.transaction_type == "DEBIT")
    df_total_txn=df_debit_txns.groupBy("customer_id").agg(sum("amount").alias("Total_amount"))
    df=df_total_txn.withColumn("customer_segment",
                                 when(col("Total_amount") >= 100000, "Premium")
                                 .when((col("Total_amount") >= 50000) & (col("Total_amount") < 100000), "Gold")
                                 .when(col("Total_amount") < 50000, "Regular"))
    return df

def fraud_detection_flags(df_transaction):
    df=df_transaction.groupBy("account_id").agg(count("transaction_id").alias("total_transactions")).select("transaction_id", "amount")
    print(df.columns)
    df=df.withColumn("fraud_flag",
                    when(col("amount") > 50000, "True")
                    .when(col("total_transactions") > 10, "True"))
    df.show()
    df.toPandas().to_csv(r"C:/Users/meena/OneDrive/Desktop/Banking ETL Project/data/curated/fraud_flags.csv", index=False)
    
from pyspark.sql.functions import sum, to_date
from pyspark.sql.window import Window

def daily_balance_trend(df_transaction):
    df_date = df_transaction.withColumn("date", to_date("timestamp"))
    df_daily_total=df_date.groupBy("account_id", "date").agg(sum("amount").alias("daily_total"))

    window_spec=Window.partitionBy("account_id").orderBy("date")
    df_running_total=df_daily_total.withColumn("running_balance", sum("daily_total").over(window_spec))
    return df_running_total
    

#total_spent_per_customer(df_transaction, df_account)
monthly_transaction_summary(df_transaction)
#account_activity_report(df_transaction)
#customer_seggregation(df_transaction, df_account)
#fraud_detection_flags(df_transaction)#
#daily_balance_trend(df_transaction)