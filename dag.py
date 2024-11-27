from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys

# Ensure the functions are correctly imported from the InfoGov.etl modu
sys.path.append("/home/shivam20126/")

from InfoGov.api_extraction_7102 import api7102
from InfoGov.api_extraction_7103 import api7103
from InfoGov.api_extraction_7107 import api7107

from InfoGov.etl.transform_7102 import t7102
from InfoGov.etl.transform_7103 import t7103
from InfoGov.etl.transform_7107 import t7107

from InfoGov.etl.load_t1 import load_t1
from InfoGov.etl.load_t2 import load_t2
from InfoGov.etl.load_t4 import load_t4
from InfoGov.etl.load_t5 import load_t5


# Define default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 11, 8),
    "email": ["shivam20126@iiitd.ac.in"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    "API_Extraction",
    default_args=default_args,
    description="Extraction API",
    schedule_interval=timedelta(days=1),
)

api7102 = PythonOperator(
    task_id="api7102",
    python_callable=api7102,
    dag=dag,
)

api7103 = PythonOperator(
    task_id="api7103",
    python_callable=api7103,
    dag=dag,
)


api7107 = PythonOperator(
    task_id="api7107",
    python_callable=api7107,
    dag=dag,
)


t7102 = PythonOperator(
    task_id="t7102",
    python_callable=t7102,
    dag=dag,
)


t7103 = PythonOperator(
    task_id="t7103",
    python_callable=t7103,
    dag=dag,
)


t7107 = PythonOperator(
    task_id="t7107",
    python_callable=t7107,
    dag=dag,
)


load_t1 = PythonOperator(
    task_id="load_t1",
    python_callable=load_t1,
    dag=dag,
)

load_t2 = PythonOperator(
    task_id="load_t2",
    python_callable=load_t2,
    dag=dag,
)


# Create the PythonOperator tasks
load_t4 = PythonOperator(
    task_id="load_t4",
    python_callable=load_t4,
    dag=dag,
)

load_t5 = PythonOperator(
    task_id="load_t5",
    python_callable=load_t5,
    dag=dag,
)

# without python callable
branch1 = DummyOperator(task_id="branch1")
branch2 = DummyOperator(task_id="branch2")




# Set the task dependencies
api7102 >> branch1
api7103 >> branch1
api7107 >> branch1

branch1 >> t7102
branch1 >> t7103
branch1 >> t7107

t7102 >> branch2
t7103 >> branch2
t7107 >> branch2

branch2 >> load_t1
branch2 >> load_t2
branch2 >> load_t4
branch2 >> load_t5