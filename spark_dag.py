from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(seconds=10)
}


with DAG("spark_dag", start_date=datetime(2021, 1 ,1), 
    schedule_interval="@daily", default_args=default_args, catchup=False) as dag:


    saving_rates = BashOperator(
        task_id="save_romeo_file",
        bash_command="""
            hdfs dfs -mkdir -p /test_spark && \
            hdfs dfs -put -f /romeo-and-juliet.txt /test_spark
        """
    )

    creating_forex_rates_table = HiveOperator(
        task_id="create_romeo_tab",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS ROMEO_COUNT(
                WORD STRING,
                NUMBER_OF_OCC INT
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """
    )

    spark_job = SparkSubmitOperator(
        task_id="spark_job",
        application="/opt/airflow/dags/scripts/spark_job.py",
        conn_id="spark_conn",
        verbose=False
    )


saving_rates >> creating_forex_rates_table >> spark_job