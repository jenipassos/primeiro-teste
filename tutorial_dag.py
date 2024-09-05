from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.branch import BaseOperator
import pandas as pd
import requests
import json

def captura_conta_dados():
    url = "https://homologacao.agroopenbank.com.br/api/v1/produtores?key=fpGCVXMudaBEmA46BgRG7LLrHqvQDeSb"
    response = requests.get(url)
    df = pd.DataFrame(json.loads(response.content))
    return df

def id():
    id =


with DAG('tutorial_dag', start_date = datetime(2022,11,07),
         schedule_interval = '30 * * * *', catchup= False) as dag:

    captura_conta_dados = PythonOperator(
        task_id = 'captura_conta_dados',
        python_callable = captura_conta_dados
    )

    e_valida = BranchPythonOperator(
        task_id = 'e_valida',
        python_callable = e_valida
    )

    valido = BaseOperator(
        task_id = 'valido',
        bash_command = "echo 'Quantidade OK'"
    )

    nvalido = BaseOperator(
        task_id ='nvalido',
        bash_command ="echo 'Quantidade não OK'"
    )