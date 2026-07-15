from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("DataIngestion").getOrCreate()
df_account=spark.read.csv("C:/Users/meena/OneDrive/Desktop/Banking ETL Project/data/raw/accounts.csv", header=True, inferSchema=True)
df_customer=spark.read.csv("C:/Users/meena/OneDrive/Desktop/Banking ETL Project/data/raw/customers.csv", header=True, inferSchema=True)
df_transaction=spark.read.csv("C:/Users/meena/OneDrive/Desktop/Banking ETL Project/data/raw/transactions.csv", header=True, inferSchema=True)

"""top_5_customers=df_customer.head(5)
top_5_accounts=df_account.head(5)
top_5_transactions=df_transaction.head(5)
print(top_5_customers)
print(top_5_accounts)
print(top_5_transactions)"""

df_account.printSchema()
df_customer.printSchema()
df_transaction.printSchema()