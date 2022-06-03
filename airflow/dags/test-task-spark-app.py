from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'sz',
    'depends_on_past': False,
    'start_date': pendulum.yesterday(),
    'email': ['sz@sz.sz'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
with DAG(
        default_args=default_args,
        dag_id="test-task-spark-app",
        schedule_interval="*/15 * * * *",
        concurrency=False,
        catchup=False,
        description="load the data from kafka, enrich with dictionary and store it to hdfs storage",
) as dag:
    spark_operator = SparkSubmitOperator(
        conn_id="kubernetes-connection",
        jars="/opt/jars/test-task-shaded.jar",
        java_class="com.github.sz763.SparkApplication",
        dag=dag,
        env_vars={
            "BOOTSTRAP_SERVERS": Variable.get("kafka_bootstrap_servers"),
            "CLIENT_ID": Variable.get("kafka_client_id"),
            "GROUP_ID": Variable.get("kafka_group_id"),
            "TOPIC_NAME": Variable.get("kafka_topic_name"),
            "WRITE_TO": Variable.get("hdfs_storage"),
            "JDBC_URL": Variable.get("db.jdbc_url"),
            "DB_USER": Variable.get("db_user"),
            "DB_PASSWORD": Variable.get("db_password"),
            "DB_DRIVER": Variable.get("db_driver"),
            "BATCH_SIZE": Variable.get("batch_size", 5000),
            "PARQUET_BATCH_SIZE": Variable.get("parquet_batch_size", 5000),
        }
    )
