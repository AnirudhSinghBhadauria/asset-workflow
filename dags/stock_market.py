import requests
from astro import sql as aql
from astro.files import File
from datetime import datetime
from airflow.hooks.base import BaseHook
from airflow.decorators import dag, task
from astro.sql.table import Table, Metadata
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from include.stock_market.tasks import (
     BUCKET_NAME,
     _get_stock_prices, 
     _store_prices, 
     _get_formatted_csv
)

SYMBOL = 'AAPL'

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
          task_id='is_api_available',
          poke_interval = 40,
          timeout = 300,
          mode = 'poke'
     )
     def is_api_available() -> PokeReturnValue:
          api = BaseHook.get_connection("stock_api")
          url = f"{api.host}{api.extra_dejson['endpoint']}"
          headers = api.extra_dejson['headers']
          response = requests.get(url, headers=headers)
          
          condition = response.json()['finance']['result'] is None
          
          return PokeReturnValue(
               is_done=condition,
               xcom_value=url
          )
     
     api_available = is_api_available()
     
     get_stock_prices = PythonOperator(
          task_id = 'get_stock_prices',
          python_callable = _get_stock_prices,
          op_kwargs = {
               # at runtime this {{..}} will be replace with "https://query1.finance.yahoo.com/"
               'url': '{{ task_instance.xcom_pull(task_ids="is_api_available") }}',
               'symbol': SYMBOL
          }
     )
     
     store_prices = PythonOperator(
          task_id = 'store_prices',
          python_callable = _store_prices,
          op_kwargs = {
               # We are passing this stock to the callable python function
               'stock': '{{ task_instance.xcom_pull(task_ids="get_stock_prices") }}'
          }
     )
     
     format_prices = DockerOperator(
          task_id = 'format_prices',
          image = 'airflow/stock-app',
          container_name = 'format_prices',
          api_version = 'auto',
          auto_remove = True,
          docker_url = 'tcp://docker-proxy:2375',
          network_mode = 'container:spark-master',
          tty = True,
          xcom_all = False,
          mount_tmp_dir = False,
          environment = {
               'SPARK_APPLICATION_ARGS': '{{ task_instance.xcom_pull(task_ids="store_prices") }}'
          }
          
     )
     
     get_formatted_csv = PythonOperator(
          task_id = 'get_formatted_csv',
          python_callable = _get_formatted_csv,
          op_kwargs = {
               'path': '{{ task_instance.xcom_pull(task_ids="store_prices") }}'
          }
     )
     
     load_to_dw = aql.load_file(
          task_id = 'load_to_dw',
          input_file = File(
               path = f"s3://{BUCKET_NAME}/{{{{ task_instance.xcom_pull(task_ids='get_formatted_csv') }}}}", conn_id = 'minio'
          ),
          output_table = Table(
               name = 'stock_market',
               conn_id = 'postgres',
               metadata = Metadata(
                    schema = 'public'
               )
          )
     )
               
     api_available >> get_stock_prices >> store_prices >> format_prices >> get_formatted_csv >> load_to_dw
               
stock_market()
