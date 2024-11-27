# from datetime import timedelta
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.utils.dates import days_ago
# from datetime import datetime

# import api_extraction_7102
# import api_extraction_7103
# import api_extraction_7107

# import etl.transform_7102
# import etl.transform_7103
# import etl.transform_7107


# import etl.load_t1
# import etl.load_t2
# import etl.load_t4
# import etl.load_t5


# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2020, 11, 8),
#     'email': ['shivam20126@iiitd.ac.in'],
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=1)
# }

# dag = DAG(
#     'API_Extraction',
#     default_args=default_args,
#     description='Extraction API',
#     schedule_interval=timedelta(days=1),
# )

# api_7102 = PythonOperator(
#     task_id='api_extraction_7102',
#     python_callable=api_extraction_7102,
#     dag=dag,
# )

# api_7103 = PythonOperator(
#     task_id='api_extraction_7103',
#     python_callable=api_extraction_7103,
#     dag=dag,
# )

# api_7107 = PythonOperator(
#     task_id='api_extraction_7107',
#     python_callable=api_extraction_7107,
#     dag=dag,
# )

# transform_7102 = PythonOperator(
#     task_id='transform_7102',
#     python_callable=etl.transform_7102,
#     dag=dag,
# )

# transform_7103 = PythonOperator(
#     task_id='transform_7103',
#     python_callable=etl.transform_7103,
#     dag=dag,
# )

# transform_7107 = PythonOperator(
#     task_id='transform_7107',
#     python_callable=etl.transform_7107,
#     dag=dag,
# )

# load_t1 = PythonOperator(
#     task_id='load_t1',
#     python_callable=etl.load_t1,
#     dag=dag,
# )


# load_t2 = PythonOperator(
#     task_id='load_t2',
#     python_callable=etl.load_t2,
#     dag=dag,
# )


# load_t4 = PythonOperator(
#     task_id='load_t4',
#     python_callable=etl.load_t4,
#     dag=dag,
# )


# load_t5 = PythonOperator(
#     task_id='load_t5',
#     python_callable=etl.load_t5,
#     dag=dag,
# )



from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'my_sample_dag',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),  # Run daily
    start_date=datetime(2024, 11, 27),  # Start date of the DAG
    catchup=False,  # Prevents backfilling for dates before the start_date
)

# Define tasks
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

def my_python_function():
    print("Hello from Python task!")

python_task = PythonOperator(
    task_id='my_python_task',
    python_callable=my_python_function,
    dag=dag,
)

end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Set task dependencies
start_task >> python_task >> end_task
