from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from include.scheduling.tasks import _task_dependency

def on_success(dict):
     print(dict)

def on_failur(dict):
     print(dict)

@dag(
     start_date = datetime(25, 1, 1),
     schedule = '@daily',
     catchup = False,
     dagrun_timeout = timedelta(seconds = 60),
     on_success_callback = on_success,
     on_failure_callback = on_failur,
     tags = ['advance scheduling in airflow']
)
def scheduling_basics():     
     @task(retries = 15)
     def print_name():
          print("hi my name is anirudh!")
          
          return 50
          
     print_name = print_name()
     
     task_dependency = PythonOperator(
          task_id = 'task_dependency',
          python_callable = _task_dependency,
          depends_on_past = True,
          op_kwargs = {
               'value': '{{ task_instance.xcom_pull(task_id = "print_name") }}'
          }
     )
     
     print_name >> task_dependency
     
scheduling_basics()
