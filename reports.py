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

drop_userdump = BashOperator(task_id= 'drop_userdump', bash_command=""" impala-shell -q "drop table if exists practical_exercise_1.user_upload_dump" """, dag=dag)


create_userdump=BashOperator(task_id='create_userdump', bash_command=""" impala-shell -q "create external table practical_exercise_1.user_upload_dump(user_id int, file_name STRING, timestamps int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/cloudera/exe1/upload' tblproperties ('skip.header.line.count'='1');" """, dag=dag)


drop_tables = BashOperator(task_id='drop_user_report', bash_command=""" impala-shell -q "drop table if exists practical_exercise_1.user_report;"
impala-shell -q "drop table if exists practical_exercise_1.sums;"
impala-shell -q "drop table if exists practical_exercise_1.join_sums_counts;"
impala-shell -q "drop table if exists practical_exercise_1.find_isactive;"
impala-shell -q "drop table if exists practical_exercise_1.upload_counts;" """, dag=dag)

init_now = BashOperator(task_id='init_now', bash_command=""" NOW=$(date +"%s") """, dag=dag)

set_now=BashOperator(task_id='set_now', bash_command=""" impala-shell -q "set var:now=$NOW;" """,dag=dag) 

create_sums = BashOperator(task_id='create_sums', bash_command=""" impala-shell -q "create table practical_exercise_1.sums as select user_id, sum(ins) total_updates, sum(ins2) total_inserts, sum(ins1) total_deletes from (select user_id, if(type='UPDATE',cnt,0) ins, if(type='DELETE', cnt, 0) ins1, if(type='INSERT',cnt,0) ins2 from (select user_id,type,count(*) as cnt from practical_exercise_1.activitylog group by user_id,type)a)b group by user_id;" """ , dag=dag)


create_uploadcount = BashOperator(task_id='create_uploadcount', bash_command=""" impala-shell -q "create table practical_exercise_1.upload_counts as select user_id, count(*) as cnt from practical_exercise_1.user_upload_dump group by user_id;"
if [[ $? -ne 0 ]] ; then
    echo "Could not overwrite with upload_count"
    exit 1
fi """, dag=dag)

create_joinsums = BashOperator(task_id='create_joinsums', bash_command = """ impala-shell -q "create table practical_exercise_1.join_sums_counts as select sums.user_id, sums.total_updates,sums.total_deletes,sums.total_inserts, upload_counts.cnt upload_count from practical_exercise_1.sums join practical_exercise_1.upload_counts on upload_counts.user_id=sums.user_id;" """, dag=dag)


create_findisactive = BashOperator(task_id='create_findisactive', bash_command = """ impala -q "create table practical_exercise_1.find_isactive as select join_sums_counts.user_id, join_sums_counts.total_updates, join_sums_counts.total_inserts, join_sums_counts.total_deletes, join_sums_counts.upload_count, d.last_active_type, d.is_active from practical_exercise_1.join_sums_counts join (select c.user_id, t, activitylog.type last_active_type, c.is_active from (select activitylog.user_id, max(timestamps) t, if($NOW-max(timestamps)<=172800,'TRUE','FALSE') is_active from practical_exercise_1.activitylog group by user_id)c right outer join practical_exercise_1.activitylog where c.t=activitylog.timestamps and c.user_id=activitylog.user_id)d on d.user_id=join_sums_counts.user_id;"
if [[ $? -ne 0 ]] ; then
    echo "Could not create find_isactive"
    exit 1
fi """,dag = dag)



create_userreport = BashOperator(task_id='create_userreport',bash_command=""" impala-shell -q "create table practical_exercise_1.user_report as select user.id, find_isactive.total_updates, find_isactive.total_inserts, find_isactive.total_deletes,find_isactive.upload_count,find_isactive.last_active_type,find_isactive.is_active from practical_exercise_1.find_isactive right outer join practical_exercise_1.user on user.id=find_isactive.user_id;"


if [[ $? -ne 0 ]] ; then
    echo "Could not create user_report"
    exit 1
fi """,dag=dag)


user_totaltable = BashOperator(task_id='user_totaltable', bash_command=""" impala-shell -q "create table if not exists practical_exercise_1.user_total(time_ran timestamp, total_users bigint, users_added bigint);"

if [[ $? -ne 0 ]] ; then
    echo "Could not create the table user_total"
    exit 1
fi """,dag=dag)

insert_usertotal = BashOperator(task_id='insert_usertotal', bash_command=""" impala-shell -q "insert into practical_exercise_1.user_total select current_timestamp(), sub1.t , case when sub2.t1 is NULL then sub1.t when sub2.t1 is not NULL then sub1.t-sub2.t1 end from (select count(distinct id) as t from practical_exercise_1.user)sub1, (select max(total_users) t1 from user_total) sub2;" """,dag=dag)








load_data.set_downstream(addition_data)
load_data.set_downstream(import_user)
create_csv.set_downstream(move_csv_to_hdfs)
move_csv_to_hdfs.set_downstream(drop_userdump)
drop_userdump.set_downstream(create_userdump)
move_csv_to_hdfs.set_downstream(move_csv_to_archive)
init_now.set_downstream(set_now)
set_now.set_downstream(create_findisactive)
addition_data.set_downstream(create_sums)
addition_data.set_downstream(create_uploadcount)
drop_tables.set_downstream(create_sums)
drop_tables.set_downstream(create_uploadcount)
create_sums.set_downstream(create_joinsums)
create_uploadcount.set_downstream(create_joinsums)
create_joinsums.set_downstream(create_findisactive)
create_findisactive.set_downstream(create_userreport)
user_totaltable.set_downstream(insert_usertotal)
set_now.set_downstream(insert_usertotal)
import_user.set_downstream(user_totaltable)
create_userdump.set_downstream(create_uploadcount)










    


