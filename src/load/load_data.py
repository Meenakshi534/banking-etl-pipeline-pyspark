from src.transformation.transform_data import total_spent_per_customer


import pyspark
import os


os.environ["PYSPARK_PYTHON"] = r"C:\Users\meena\AppData\Local\Programs\Python\Python310\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\meena\AppData\Local\Programs\Python\Python310\python.exe"


os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
spark = SparkSession.builder.appName("DataIngestion").getOrCreate()
from src.utils.logger import logger

df_account = spark.read.csv(r"C:/Users/meena/OneDrive/Desktop/Banking ETL Project/data/processed/accounts.csv", header=True, inferSchema=True)
df_customer = spark.read.csv("C:/Users/meena/OneDrive/Desktop/Banking ETL Project/data/processed/customers.csv", header=True, inferSchema=True)
df_transaction = spark.read.csv("c:/Users/meena/OneDrive/Desktop/Banking ETL Project/data/processed/transactions.csv", header=True, inferSchema=True)


def load_to_csv(df, output_path):
    df.toPandas().to_csv(f"C:/Users/meena/OneDrive/Desktop/Banking ETL Project/data/curated/{output_path}.csv", index=False)
    logger.info(f"Saved {output_path}.csv successfully")

#load_to_csv(total_spent_per_customer(df_transaction, df_account), 'customer_spending_2')