from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.macros import ds_add
import os
from os.path import join
import pandas as pd

with DAG(
    'extrai_dados_climaticos',
    start_date=pendulum.datetime(2022, 12, 1, tz="UTC"),
    schedule_interval='0 0 * * 1' # executar toda segunda-feira
) as dag:
    cria_pasta = BashOperator(
        task_id='cria_pasta',
        bash_command='mkdir -p "/home/joao/airflow/semana={{data_interval_end.strftime("%Y-%m-%d")}}"'
    )

    def baixa_dados_semanais(data_interval_end: str) -> None:
        data_inicio = data_interval_end
        data_fim = ds_add(data_interval_end, 7)
        file_path = f'/home/joao/airflow/semana={data_interval_end}/'

        city = 'Boston'
        key = os.environ.get('VISUAL_CROSSING_KEY', '')
        

        URL = join("https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/",
        f"{city}/{data_inicio}/{data_fim}?unitGroup=metric&include=days&key={key}&contentType=csv")

        dados = pd.read_csv(URL)

        dados.to_csv(file_path + 'raw_data.csv')
        dados[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperaturas.csv')
        dados[['datetime', 'description', 'icon']].to_csv(file_path + 'condicoes.csv')


    baixa_dados = PythonOperator(
        task_id='baixa_dados_semanais',
        python_callable=baixa_dados_semanais,
        op_kwargs={'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'}
    )

    cria_pasta >> baixa_dados
