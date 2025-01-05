import random
from datetime import datetime
from airflow.decorators import dag, task
 
 
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags = ['A simple DAG to generate and check random numbers']
)
def random_number_checker():
     
     @task
     def generate_random_number():
          number = random.randint(1, 100)
          print(f"Generated random number: {number}")
          
          return number
     
     @task
     def check_even_odd(generated_number):
          result = "even" if generated_number % 2 == 0 else "odd"
          print(f"The number {generated_number} is {result}.")
     
     check_even_odd(generate_random_number())

random_number_checker()
