from airflow import DAG
from datetime import datetime
from airflow.utils.state import State
from airflow.models import TaskInstance
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


with DAG(
    dag_id='AwsS3_Pyspark_ETL_Dag',
    start_date=datetime(2025,8,3),
    schedule_interval=None,
    catchup=False) as dag:


    spark_bronze_layer_task = SparkSubmitOperator(
        task_id='spark_bronze_layer',
        application='/home/airflowsup/Airflow_AwsS3_Project/Airflow_AwsS3_Pyspark_BronzeLayerScript.py',
        conn_id='spark_local',
        conf={
            "spark.master": "local",
            "spark.driver.bindAddress": "127.0.0.1",
            "spark.ui.enabled": "false"
            # "spark.ui.port": "18080",  # Pick a port not in use
            # "spark.port.maxRetries": "50"  # Optional: increase retries
        },
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        verbose=True,
        dag=dag
    )
    spark_silver_layer_task = SparkSubmitOperator(
        task_id='spark_silver_layer',
        application='/home/airflowsup/Airflow_AwsS3_Project/Airflow_AwsS3_Pyspark_SilverLayerScript.py',
        conn_id='spark_local',
        conf={
            "spark.master": "local",
            "spark.driver.bindAddress": "127.0.0.1",
            "spark.ui.enabled": "false"
            # "spark.ui.port": "18080",  # Pick a port not in use
            # "spark.port.maxRetries": "50"  # Optional: increase retries
        },
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        verbose=True,
        dag=dag
    )
    spark_gold_layer_task = SparkSubmitOperator(
        task_id='spark_gold_layer',
        application='/home/airflowsup/Airflow_AwsS3_Project/Airfloiw_AwsS3_Pyspark_GoldLayerScript.py',
        conn_id='spark_local',
        conf={
            "spark.master": "local",
            "spark.driver.bindAddress": "127.0.0.1",
            "spark.ui.enabled": "false"
            # "spark.ui.port": "18080",  # Pick a port not in use
            # "spark.port.maxRetries": "50"  # Optional: increase retries
        },
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        verbose=True,
        dag=dag
    )
    

    def check_bronze_condition(**context):
        dag_run = context['dag_run']
        # Get the current DAG
        dag = context['dag']
        # Get the task instance for 'spark_bronze_layer'
        ti = TaskInstance(
            task=dag.get_task('spark_bronze_layer'),
            execution_date=dag_run.execution_date
        )
        ti.refresh_from_db()
        # Return True if bronze layer succeeded, else False (skip downstream)
        return ti.state == State.SUCCESS


    short_circuit_bronze = ShortCircuitOperator(
        task_id='short_circuit_bronze',
        python_callable=check_bronze_condition,
        provide_context=True,
        trigger_rule='all_done',
        dag=dag
)
    def check_silver_condition(**context):
        dag_run = context['dag_run']
        dag = context['dag']
        ti = TaskInstance(
            task=dag.get_task('spark_silver_layer'),
            execution_date=dag_run.execution_date
        )
        ti.refresh_from_db()
        return ti.state == State.SUCCESS
    
    
    short_circuit_silver = ShortCircuitOperator(
        task_id='short_circuit_silver',
        python_callable=check_silver_condition,
        provide_context=True,
        trigger_rule='all_done',
        dag=dag
    )
    
    spark_bronze_layer_task >> short_circuit_bronze >> spark_silver_layer_task >> short_circuit_silver >> spark_gold_layer_task
