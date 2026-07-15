from pyspark.sql import SparkSession
from config.config import CUSTOMERS_FILE, ACCOUNTS_FILE, TRANSACTIONS_FILE

import pyspark
import os
import logging
logger = logging.getLogger(__name__)


os.environ["PYSPARK_PYTHON"] = r"C:\Users\meena\AppData\Local\Programs\Python\Python310\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\meena\AppData\Local\Programs\Python\Python310\python.exe"


os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"

spark = SparkSession.builder \
    .appName("Banking ETL") \
    .getOrCreate()

def read_customers():
    try:
        df = spark.read.csv(CUSTOMERS_FILE, header=True, inferSchema=True)
        logger.info(f"successfully read customers data from {CUSTOMERS_FILE}")
        return df
    except Exception as e:
        logger.error(f"Error reading customers data from {CUSTOMERS_FILE}: {e}")
        raise


def read_accounts():
    try:
        df=spark.read.csv(ACCOUNTS_FILE, header=True, inferSchema=True)
        logger.info(f"successfully read accounts data from {ACCOUNTS_FILE}")
        return df
    except Exception as e:
        logger.error(f"Error reading acoounts data from {ACCOUNTS_FILE}: {e}")
        raise
def read_transactions():
    return spark.read.csv(TRANSACTIONS_FILE, header=True, inferSchema=True)