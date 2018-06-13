
# Practical-Exercise-Airflow and Impala

The goal of the exercise is to generate two tables to report the user statistics of a website including the number of inserts, updates, deletes made by the user, whether the user has been active for the past two days, etc and also to record the number of total users in the system and the number of users added from time to time.

### practical_exercise_data_generator.py

- Allows you to repeatedly create a csv file and to load two tables user and activitylog into mysql database. 

- The table `user` has two fields: 
    - **id** - Used to uniquely identify a user in the table. 
    - **name** - indicates the name of the user.

- The table `activitylog` has four fields: 
    - **id** -  indicates the identifier for the activity event.  
    - **user_id** - used to identify a user in the table.
    - **type** - indicates the type of activity - update, insert or delete.
    - **timestamp** - indicates the timestamp at which the particular activity was performed by the user.

### Instructions to run:
Initialization.py and Reports.py should be placed inside the /airflow/dags/ folder. 

#### ./start.sh - To initialize the web server, scheduler and the worker.

#### Initialization.py

`airflow unpause initialization`

`airflow trigger_dag initialization`

	- Creates the DB and initializes the sqoop metastore.

#### Reports.py

`airflow unpause reports`

`airflow trigger_dag reports`

- A DAG is initialized with an array of tasks to construct two tables user_report and user_total
	
- The user_report table contains the user id, number of updates, inserts and the deletes that the user has made, the last activity type of the user (Whether the user has done an insert, update or a delete), whether he has been active during the last two days and the number of uploads he has made.

- The user_total table is created to determine the number of users using the website at a certain point of time. Every time the DAG is run, a new row gets appended at the end of the user_total which records the timestamp at which the entry is made, total number of users, the number of users added.






