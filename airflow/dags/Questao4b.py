from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
import pendulum

default_args = {
    'owner': 'Rafael',
    'depends_on_past': False,
    'start_date': pendulum.today('UTC').add(weeks=-1),  
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag2 = DAG(
    'Questao4b',
    default_args=default_args,
    description='Process CSV and generate report',
    schedule=[Dataset('/tmp/vendas.csv')],  
)

def process_csv_and_print_report(**kwargs):
    df = pd.read_csv('/tmp/vendas.csv')


    df_grouped = df.groupby('funcionario_nome')['preco_de_venda'].sum().reset_index()


    df_grouped = df_grouped.sort_values(by='preco_de_venda', ascending=False).reset_index(drop=True)

    report = ""
    for index, row in df_grouped.iterrows():
        report += f"Vendedor {row['funcionario_nome']} faturou o valor {row['preco_de_venda']} e ficou em {index + 1} lugar.\n"

    print(report)

report_task = PythonOperator(
    task_id='process_csv_and_print_report',
    python_callable=process_csv_and_print_report,
    dag=dag2,
)

report_task
