from airflow.operators import BashOperator
from airflow.models import DAG
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(minutes=1),
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('initialization', default_args=default_args, schedule_interval=None, start_date=datetime.now() - timedelta(minutes=1))


create_db = BashOperator(
    task_id='create_db',
    bash_command=""" impala-shell -q "create database if not exists practical_exercise_1;" """,
    dag=dag)


meta_store = BashOperator(
    task_id='meta_store',
    bash_command=""" nohup sqoop metastore & """,
    dag=dag)

sqoop_job = BashOperator(
    task_id='sqoop_job',
    bash_command=""" sqoop job --meta-connect jdbc:hsqldb:hsql://localhost:16000/sqoop --create practical_exercise_1.activitylog -- import --connect jdbc:mysql://localhost/practical_exercise_1 --username root --password-file /user/cloudera/pwd.txt  --table activitylog -m 2 --hive-import --hive-database practical_exercise_1 --hive-table activitylog --incremental append --check-column id --last-value 0 """, dag=dag)

create_db.set_downstream(sqoop_job)
meta_store.set_downstream(sqoop_job)














    


