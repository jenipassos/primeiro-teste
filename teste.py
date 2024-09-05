from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

def hello():
    print("Hello Word")

def tudobem():
    print("Tudo bem?")

with DAG('teste', start_date = datetime(2022,10,20),
         schedule= '30 * * * *', catchup= False) as dag:

  helloWorld = PythonOperator(
      task_id= 'Hello_Word',
      python_callable= hello
  )

  Comoesta = PythonOperator(
      task_id='Tudo_Bem',
      python_callable=tudobem
  )

  helloWorld >> Comoesta