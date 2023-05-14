from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 9),
    'retries': 2,
    'retry_delay': timedelta(seconds=10),
}

# Define the DAG
dag = DAG('main_dag', default_args=default_args, schedule=None)

# Define the paths to the PySpark scripts
spark_producer = "/opt/airflow/app/spark_producer.py"
spark_consumer = "/opt/airflow/app/kafka_mongo.py"
mongo_to_elk = "/opt/airflow/app/mongo_to_elk.py"

# Define the SparkOperator tasks
write_to_kafka = SparkSubmitOperator(
    packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.2',
    task_id='write_to_kafka',
    application=spark_producer,
    conn_id='spark_default',
    verbose=False,
    dag=dag
)

write_to_mongodb = SparkSubmitOperator(
    packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.2,' \
             'org.mongodb.spark:mongo-spark-connector:10.0.0',
    task_id='write_to_mongodb',
    application=spark_consumer,
    conn_id='spark_default',
    verbose=False,
    dag=dag
)

write_to_kibana = SparkSubmitOperator(
    packages='org.mongodb.spark:mongo-spark-connector:10.0.2,' \
             'org.elasticsearch:elasticsearch-spark-30_2.12:8.0.0',
    task_id='write_to_kibana',
    application=mongo_to_elk,
    conn_id='spark_default',
    verbose=False,
    dag=dag
)

# Set the order of the tasks
write_to_kafka >> write_to_mongodb >> write_to_kibana