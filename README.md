# Banking ETL Pipeline using PySpark

## Project Overview

Built an end-to-end ETL pipeline using PySpark.

## Tech Stack
- Python
- PySpark
- Spark SQL
- Pandas
- Faker
- Git
- VS Code

## Architecture
Raw → Processed → Curated

## Features
- Data Validation
- Duplicate Removal
- Orphan Record Removal
- Customer Segmentation
- Fraud Detection
- Monthly Transaction Summary
- Running Balance Calculation

## Project Structure
Banking ETL Project/
│
├── config/
│   ├── __init__.py
│   └── config.py
│
├── data/
│   ├── raw/
│   ├── processed/
│   └── curated/
│
├── logs/
│
├── src/
│   ├── extract/
│   ├── cleaning/
│   ├── transformation/
│   └── load/
│
├── main.py
├── requirements.txt
└── README.md

## How to Run
python main.py