#!/usr/bin/env python
# coding: utf-8

# In[1]:


from Spark_Config_Setup import get_spark
spark = get_spark()


# In[2]:


DF_Extract = spark.read.csv(r"s3a://supawsbucket07/Airflow_AwSS3_Pyspark_Project/synthetic_ecommerce_transactions.csv", header=True,inferSchema=True)
# DF_Extract.show()


# In[3]:


DF_Bronze = DF_Extract.dropDuplicates()
DF_Bronze_Reject = DF_Extract.join(DF_Bronze,on=["transaction_id"],how="left_anti")
DF_Bronze_Reject.write.format("parquet").mode("overwrite").save(r"s3a://supawsbucket07/Airflow_AwSS3_Pyspark_Project/bronze/valid/Bronze_Layer_Reject.parquet")
DF_Bronze.write.format("parquet").mode("overwrite").save(r"s3a://supawsbucket07/Airflow_AwSS3_Pyspark_Project/bronze/rejects/Bronze_Layer_Data.parquet")


# In[5]:


# get_ipython().system('jupyter nbconvert --to script /home/airflowsup/Airflow_AwsS3_Project/Airflow_AwsS3_Project_PysparkScript.ipynb --output /home/airflowsup/Airflow_AwsS3_Project/Airflow_AwsS3_Project_PysparkScript')

