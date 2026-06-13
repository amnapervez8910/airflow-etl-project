from airflow import DAG
from airflow.operator.python import PythonOperator
from datetime import datetime
import csv
import random

# 1. Add new data (Generate random sales data and save to CSV)
def add_data():
    products = ['Laptop', 'Mobile', 'Tablet', 'Headphones', 'Camera']

    # Open CSV file in write mode
    with open('/workspaces/airflow-etl-project/sales.csv', 'w', newline='') as f:
        writer = csv.writer(f)

        # Write header row
        writer.writerow(['product', 'price', 'quantity'])

        # Generate 15 random records
        for _ in range(15):
            writer.writerow([
                random.choice(products),          # Random product
                random.randint(5000, 100000),     # Random price
                random.randint(1, 10)             # Random quantity
            ])


# 2. Extract (Read data from CSV and push to XCom)
def extract(ti):
    data = []

    # Read data from CSV file
    with open('/workspaces/airflow-etl-project/sales.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            data.append(row)

    # Push extracted data to XCom
    ti.xcom_push(key='data', value=data)


# 3. Transform (Process data: calculate total sales and best product)
def transform(ti):
    # Pull data from XCom
    data = ti.xcom_pull(key='data', task_ids='extract_task')

    total_sales = 0
    product_count = {}

    # Process each row
    for row in data:
        try:
            price = int(row['price'])
            quantity = int(row['quantity'])
            product = row['product']
        except:
            # Skip invalid rows
            continue

        # Calculate total sales
        total_sales += price * quantity

        # Count product quantities
        if product in product_count:
            product_count[product] += quantity
        else:
            product_count[product] = quantity

    # Find best-selling product
    best_product = max(product_count, key=product_count.get) if product_count else "None"

    # Push result to XCom
    ti.xcom_push(key='result', value={
        "total_sales": total_sales,
        "best_product": best_product
    })


# 4. Load (Save processed result into summary CSV)
def load(ti):
    # Pull result from XCom
    result = ti.xcom_pull(key='result', task_ids='transform_task')

    # Write final output to CSV
    with open('/workspaces/airflow-etl-project/summary.csv', 'w', newline='') as f:
        writer = csv.writer(f)

        # Write header and result
        writer.writerow(['Total Sales', 'Best Product'])
        writer.writerow([result['total_sales'], result['best_product']])


# DAG Definition (Defines workflow and scheduling)
with DAG(
    dag_id='sales_etl_pipeline',
    start_date=datetime(2026, 4 , 1),
    schedule_interval='@daily',   # Run every day
    catchup=False,                # Do not run past missed schedules
    max_active_runs=1             # Only one run at a time
) as dag:

    # Define tasks
    t1 = PythonOperator(task_id='add_data', python_callable=add_data)
    t2 = PythonOperator(task_id='extract_task', python_callable=extract)
    t3 = PythonOperator(task_id='transform_task', python_callable=transform)
    t4 = PythonOperator(task_id='load_task', python_callable=load)

    # Set task dependencies (execution order)
    t1 >> t2 >> t3 >> t4