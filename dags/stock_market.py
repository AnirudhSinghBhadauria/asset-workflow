from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.sensors.base import PokeReturnValue
from airflow.hooks.base import BaseHook

def _print_stock_name():
     print("this is the stock name")
     
     price = 56
     return price

@dag(
     start_date=datetime(25, 1, 1),
     schedule='@daily',
     catchup=False,
     tags=['apple stock data pipeline'] # this tag will appear on the airlfow webui
)
# This 'stock_market' is the unique identifier of our pipeline
def stock_market():
     # First task will be to check if the api is available or not using a sensor!
     @task.sensor(
          poke_interval = 40,
          timeout = 300,
          mode = 'poke'
     )
     def is_api_available() -> PokeReturnValue:
          # We create this "stock_api" connection in metadatabase of the airflow,
          api = BaseHook.get_connection("stock_api")
               
stock_market()
