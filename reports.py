from airflow.operators import BashOperator
from airflow.models import DAG
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(minutes=1),
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG('reports', default_args=default_args, schedule_interval=None, start_date=datetime.now() - timedelta(minutes=1))

load_data = BashOperator(
    task_id='load_data',
    bash_command=""" python ~/Downloads/Exercise1/practical_exercise_data_generator.py --load_data """,
    dag=dag)

create_csv = BashOperator(
    task_id='create_csv',
    bash_command=""" python ~/Downloads/Exercise1/practical_exercise_data_generator.py --create_csv """,
    dag=dag)

addition_data = BashOperator(
    task_id='addition_data',
    bash_command=""" sqoop job --meta-connect jdbc:hsqldb:hsql://localhost:16000/sqoop --exec practical_exercise_1.activitylog """,
    dag=dag)

import_user = BashOperator(
    task_id='import_user',
    bash_command=""" sqoop import --connect jdbc:mysql://localhost/practical_exercise_1 --username root --password-file /user/cloudera/pwd.txt --table user -m 2 --hive-import --hive-overwrite --hive-database practical_exercise_1 --hive-table user """,
    dag=dag)

move_csv_to_hdfs=BashOperator(task_id='move_csv_to_hdfs',
   bash_command=""" sudo hadoop fs -put /home/cloudera/Downloads/Exercise1/upload/ /user/cloudera/exe1/ """, dag=dag)

move_csv_to_archive=BashOperator(task_id='move_csv_to_archive',bash_command=""" mv /home/cloudera/Downloads/Exercise1/upload/* /home/cloudera/Downloads/Exercise1/archive/
 """, dag=dag)

create_userdump=BashOperator(task_id='create_userdump', bash_command=""" impala-shell -q "drop table if exists practical_exercise_1.user_upload_dump; create external table practical_exercise_1.user_upload_dump(user_id int, file_name STRING, timestamps int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/cloudera/exe1/upload' tblproperties ('skip.header.line.count'='1');" """, dag=dag)


create_sums = BashOperator(task_id='create_sums', bash_command=""" impala-shell -q "drop table if exists practical_exercise_1.sums; create table practical_exercise_1.sums as select user_upload_dump.user_id, sum(ins) total_updates, sum(ins2) total_inserts, sum(ins1) total_deletes, count(user_upload_dump.user_id) upload_count from (select user_id, if(type='UPDATE',cnt,0) ins, if(type='DELETE', cnt, 0) ins1, if(type='INSERT',cnt,0) ins2 from (select user_id,type,count(*) as cnt from practical_exercise_1.activitylog group by user_id,type)a)b full outer join practical_exercise_1.user_upload_dump on user_upload_dump.user_id=b.user_id group by user_upload_dump.user_id;" """ , dag=dag)

create_findisactive = BashOperator(task_id='create_findisactive', bash_command = """ impala-shell -q "drop table if exists practical_exercise_1.find_isactive; create table practical_exercise_1.find_isactive as select sums.user_id,sums.total_updates, sums.total_inserts, sums.total_deletes, sums.upload_count, d.last_active_type, d.is_active from practical_exercise_1.sums join (select c.user_id, t, activitylog.type last_active_type, c.is_active from (select activitylog.user_id, max(timestamps) t, if(unix_timestamp()-max(timestamps)<=172800,'TRUE','FALSE') is_active from practical_exercise_1.activitylog group by user_id)c right outer join practical_exercise_1.activitylog on c.t=activitylog.timestamps where c.user_id=activitylog.user_id)d on d.user_id=sums.user_id;" """,dag = dag)



create_userreport = BashOperator(task_id='create_userreport',bash_command=""" impala-shell -q "drop table if exists practical_exercise_1.user_report; create table practical_exercise_1.user_report as select user.id, find_isactive.total_updates, find_isactive.total_inserts, find_isactive.total_deletes,find_isactive.upload_count,find_isactive.last_active_type,find_isactive.is_active from practical_exercise_1.find_isactive right outer join practical_exercise_1.user on user.id=find_isactive.user_id;" """,dag=dag)




insert_usertotal = BashOperator(task_id='insert_usertotal', bash_command=""" impala-shell -q "insert into practical_exercise_1.user_total select current_timestamp(), sub1.t , case when sub2.t1 is NULL then sub1.t when sub2.t1 is not NULL then sub1.t-sub2.t1 end from (select count(distinct id) as t from practical_exercise_1.user)sub1, (select max(total_users) t1 from user_total) sub2;" """,dag=dag)



load_data.set_downstream(addition_data)
load_data.set_downstream(import_user)
create_csv.set_downstream(move_csv_to_hdfs)
move_csv_to_hdfs.set_downstream(create_userdump)
move_csv_to_hdfs.set_downstream(move_csv_to_archive)
addition_data.set_downstream(create_sums)
create_sums.set_downstream(create_findisactive)
create_findisactive.set_downstream(create_userreport)
import_user.set_downstream(insert_usertotal)
create_userdump.set_downstream(create_sums)










    


