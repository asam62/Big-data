
from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python import PythonOperator

from lib.combine_data import combine_data_api
from lib.fetch_data_excahgerates import fetch_data_exchangerates
from lib.raw_to_format_exchangerates import convert_raw_to_formatted_exchangerates
from lib.fetch_data_mediastack import fetch_data_from_mediastack
from lib.raw_to_fmt_mediastack import convert_raw_to_formatted_mediatstack
from lib.index_elasticsearch import index_elasticsearch


with DAG(
       'my_first_dag',
       default_args={
           'depends_on_past': False,
           'email': ['airflow@example.com'],
           'email_on_failure': False,
           'email_on_retry': False,
           'retries': 1,
           'retry_delay': timedelta(seconds=15),
       },
       description='A first DAG',
       schedule_interval=None,
       start_date=datetime(2021, 1, 1),
       catchup=False,
       tags=['example'],
) as dag:
   dag.doc_md = """
       This is my first DAG in airflow.
       I can write documentation in Markdown here with **bold text** or __bold text__.
   """
   current_day = date.today().strftime("%Y%m%d")
   task1 = PythonOperator(
       task_id='fetch_data_from_exchangerates',
       python_callable=fetch_data_exchangerates,
       provide_context=True,
       op_kwargs={'task_number': 'task1'}
   )

   task2 = PythonOperator(
       task_id='convert_raw_to_formatted_exchangerates',
       python_callable=convert_raw_to_formatted_exchangerates,
       provide_context=True,
       op_kwargs={'file_name': 'money.json','current_day':  current_day}
   )
   task3 = PythonOperator(
       task_id='fetch_data_from_mediastack',
       python_callable=fetch_data_from_mediastack,
       provide_context=True,
        op_kwargs={'task_number': 'task3'}
   )
   task4 = PythonOperator(
       task_id='convert_raw_to_formatted_mediatstack',
       python_callable=convert_raw_to_formatted_mediatstack,
       provide_context=True,
       op_kwargs={'file_name': 'news.json','current_day':  current_day}
   )
   task5 = PythonOperator(
       task_id='combine_data_api',
       python_callable=combine_data_api,
       provide_context=True,
       op_kwargs={'current_day': current_day}
   )
   task6 = PythonOperator(
       task_id='index_elasticsearch',
       python_callable=index_elasticsearch,
       provide_context=True,
   )


task1.set_downstream(task2)
task3.set_downstream(task4)
task2.set_downstream(task5)
task4.set_downstream(task5)
task5.set_downstream(task6)