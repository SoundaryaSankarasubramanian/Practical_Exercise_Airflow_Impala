airflow initdb
nohup airflow webserver $* >> ~/airflow/logs/webserver.logs &
nohup airflow scheduler >> ~/airflow/logs/scheduler.logs &
nohup airflow worker $* >> ~/airflow/logs/celery.logs &

