import pandas as pd

customers = [
    [1, "Rahul Sharma", "Hyderabad", "rahulsharma@gmail.com","2005"],
    [2, "Priya Reddy", "Bangalore", "priyareddy@gmail.com","2008"],
    [3, "Amit Kumar", "Chennai", "amitkumar@gmail.com","2010"]
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

"""df = pd.DataFrame(
    customers,
    columns=["customer_id", "name", "city", "email", "customer_since"]
)

df.to_csv("C:/Users/meena/OneDrive/Desktop/Banking ETL Project/data/raw/customers.csv", index=False)
"""

from faker import Faker

fake = Faker()
"""for customer_id in range(4, 1001):
    name = fake.name()
    city = fake.city()
    email = fake.email()
    customer_since = fake.date_between(start_date = "-10y", end_date  ="today")
    customers.append([customer_id, name, city, email, customer_since])

    df=pd.DataFrame(customers, columns=["customer_id", "name", "city", "email", "customer_since"])
    df.to_csv("C:/Users/meena/OneDrive/Desktop/Banking ETL Project/data/raw/customers.csv", index=False)
    """

#generate accounts data
from random import randint, choice
account_id=1001
for customer_id in range(1, 1001):
    num_accounts = randint(1, 3)
    for _ in range(num_accounts):
        account_type=choice(["Savings", "Current"])
        accounts.append([account_id, customer_id, account_type])
        account_id +=1

df = pd.DataFrame(accounts, columns=["account_id", "customer_id", "account_type"])
df.to_csv("C:/Users/meena/OneDrive/Desktop/Banking ETL Project/data/raw/accounts.csv", index=False)


#generate transactions data
from random import randint, choice
transaction_id=5001
for account in accounts:
    account_id=account[0]
    num_transactions=randint(1,10)
    for _ in range(num_transactions):
        transaction_type=choice(["DEBIT", "CREDIT"])
        amount=randint(100, 5000)
        timestamp=fake.date_time_between(start_date="-1y", end_date="now")
        transactions.append([transaction_id, account_id, transaction_type, amount, timestamp])
        transaction_id +=1

df = pd.DataFrame(transactions, columns=["transaction_id", "account_id", "transaction_type", "amount", "timestamp"])
df.to_csv("C:/Users/meena/OneDrive/Desktop/Banking ETL Project/data/raw/transactions.csv", index=False)


print(len(accounts))