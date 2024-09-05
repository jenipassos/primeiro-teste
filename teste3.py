
from os import name
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.hooks.postgres import PostgresHook
import datetime
import requests
import json
import pandas as pd

def CapturaDados():
    src = PostgresHook(postgres_conn_id='postgres_conn')
    conn = src.get.conn()
    cursor = conn.cursor()
    url = "https://homologacao.agroopenbank.com.br/api/v1/produtores?key=fpGCVXMudaBEmA46BgRG7LLrHqvQDeSb"
    response = requests.get(url)
    df = pd.DataFrame(json.loads(response.content))
    for i in df:
        dados = df[i]
        sql = f"""""
        INSERT INTO public.dados[id, nome]
        VALUES ("{id['id']}, {name['nome']}");
        """
        cursor.execute(sql)
        conn.commit()
    cursor.close()

default_args = {
    "owner": "Layra",
    "start_date": datetime.datetime[2022,11,7]
}
