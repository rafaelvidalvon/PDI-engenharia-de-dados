from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import pendulum
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.today('UTC').add(days=-1),  
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Questao_2',
    default_args=default_args,
    description='DAG com várias tasks',
    schedule=None,
)

start_task = BashOperator(
    task_id='start_task',
    bash_command='echo "DAG Iniciada"',
    dag=dag,
)

def print_ds(**kwargs):
    ds = kwargs['ds']
    print(f"ds: {ds}")
    return ds

print_ds_task = PythonOperator(
    task_id='print_ds_task',
    python_callable=print_ds,
    dag=dag,
)

def print_ts(**kwargs):
    ts = kwargs['ts']
    print(f"ts: {ts}")
    return ts

print_ts_task = PythonOperator(
    task_id='print_ts_task',
    python_callable=print_ts,
    dag=dag,
)

dummy_task = EmptyOperator(
    task_id='dummy_task',
    dag=dag,
)

def concat_ds_ts(**kwargs):
    ti = kwargs['ti']
    ds = ti.xcom_pull(task_ids='print_ds_task')
    ts = ti.xcom_pull(task_ids='print_ts_task')
    result = f"{ds} -- {ts}"
    print(result)

concat_task = PythonOperator(
    task_id='concat_task',
    python_callable=concat_ds_ts,
    dag=dag,
)

start_task >> [print_ds_task, print_ts_task] >> dummy_task >> concat_task
