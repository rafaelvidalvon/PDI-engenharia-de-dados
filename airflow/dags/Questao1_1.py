from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Função que será executada pela task
def hello_world():
    print("Hello, world!")

# Definição da DAG
default_args = {
    'owner': 'Rafael',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Questao1',
    default_args=default_args,
    description='Teste operador',
    schedule=None,  # Substituído schedule_interval por schedule
)

# Definição da task
hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=hello_world,
    dag=dag,
)

# Ordem de execução das tasks
hello_task
