from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
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

dag1 = DAG(
    'Questao4a',
    default_args=default_args,
    description='Questao4a',
    schedule='@daily', 
)


def query_postgres_and_save_to_csv(**kwargs):
    query = """
    SELECT v.id, c.marca, c.modelo, cl.nome AS cliente_nome, f.nome AS funcionario_nome, v.data_da_venda, v.preco_de_venda
    FROM vendas v
    JOIN carros c ON v.carro_id = c.id
    JOIN clientes cl ON v.cliente_id = cl.id
    JOIN funcionarios f ON v.funcionario_id = f.id;
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()


    column_names = [desc[0] for desc in cursor.description]

    df = pd.DataFrame(results, columns=column_names)

    df.to_csv('/tmp/vendas.csv', index=False)

query_task = PythonOperator(
    task_id='query_postgres_and_save_to_csv',
    python_callable=query_postgres_and_save_to_csv,
    outlets=[Dataset('/tmp/vendas.csv')],  
    dag=dag1,
)

query_task
