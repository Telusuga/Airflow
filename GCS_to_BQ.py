import airflow
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime as dt,timedelta

dataset='composer-project-416711.landing'
table_1='employee'
table_2='department'
location='US'


query="""
create or replace table composer-project-416711.landing.empdep
as
select
a.*
from composer-project-416711.landing.employee a
join composer-project-416711.landing.department b
on a.DepartmentID=b.DepartmentID
"""



default_args={
  'retries':1,
  'retry_delay':timedelta(minutes=1)
}

dag= DAG(
  start_date=dt(2024,3,9),
  dag_id='final_test',
  schedule_interval='30 17 * * *',
  default_args=default_args,
  catchup=False,
) 
  
task_1 = GCSToBigQueryOperator(
      task_id="emp_task",
      bucket="source_bucket_20240310",
      source_objects=["employee.csv"],
      destination_project_dataset_table=f"{dataset}.{table_1}",
      schema_fields=[
          {"name": "EmployeeID", "type": "INTEGER", "mode": "NULLABLE"},
          {"name": "FirstName", "type": "STRING", "mode": "NULLABLE"},
          {"name": "LastName", "type": "STRING", "mode": "NULLABLE"},
          {"name": "Email", "type": "STRING", "mode": "NULLABLE"},
          {"name": "DepartmentID", "type": "INTEGER", "mode": "NULLABLE"},
          {"name": "Salary", "type": "STRING", "mode": "NULLABLE"},
          {"name": "JoinDate", "type": "DATE", "mode": "NULLABLE"},
      ],
      write_disposition="WRITE_TRUNCATE",
      dag=dag
  )

task_2 = GCSToBigQueryOperator(
      task_id="dept_load",
      bucket="source_bucket_20240310",
      source_objects=["departments.csv"],
      destination_project_dataset_table=f"{dataset}.{table_2}",
      schema_fields=[
          {"name": "DepartmentID", "type": "INTEGER", "mode": "NULLABLE"},
          {"name": "DepartmentName", "type": "STRING", "mode": "NULLABLE"},
      ],
      write_disposition="WRITE_TRUNCATE",
      dag=dag
  )


insert_query_job = BigQueryInsertJobOperator(
    task_id="insert_query_job",
    configuration={
        "query": {
            "query": query,
            "useLegacySql": False,
            "priority": "BATCH",
        }
    },
    location=location,
)

dummy_1=DummyOperator(
  task_id='start_job',
  dag=dag
)


dummy_2=DummyOperator(
  task_id='end_job',
  dag=dag
)


dummy_1 >> (task_1 , task_2) >> insert_query_job >> dummy_2
