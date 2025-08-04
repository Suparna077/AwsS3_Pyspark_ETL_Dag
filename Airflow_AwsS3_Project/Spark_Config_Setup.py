import findspark
import subprocess
import os
from pyspark.sql import SparkSession

# Get the first IP address (or use 'localhost' for local runs)
def get_spark():
    ip_address = subprocess.check_output(['hostname', '-I']).decode().strip().split()[0]
    print(f"Using IP address: {ip_address}")

    findspark.init()

    spark = SparkSession.builder \
        .appName("S3ReadJob") \
        .config("spark.ui.enabled", "false") \
        .config("spark.driver.bindAddress", ip_address) \
        .config("spark.driver.host", ip_address) \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    # Set AWS credentials

    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    # print(f"Access Key: {access_key}")
    # print(f"Secret Key: {secret_key}")
    if access_key and secret_key:
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
    else:
        print("AWS credentials not found in environment variables.")
    
    return spark