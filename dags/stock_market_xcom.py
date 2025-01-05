from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

def _print_stock_name():
     print("this is the stock name")
     
     price = 56
     return price

def _print_stock_price(ti = None):
     price = ti.xcom_pull(task_id = 'print_stock_price')
     print(f"This is the price of the apple stock: {price}")
     
     
with DAG(
     dag_id = 'stock_market_xcoms',
     start_date = datetime(2025, 1, 1),
     schedule = '@daily',
     catchup = False,
     tags = ['apple stock market pipeline']
):
     print_stock_name = PythonOperator(
          task_id = 'print_stock_name',
          python_callable = _print_stock_name
     )
     
     print_stock_price = PythonOperator(
          task_id = 'print_stock_price',
          python_callable = _print_stock_price
     )
     
     print_stock_name >> print_stock_price
     