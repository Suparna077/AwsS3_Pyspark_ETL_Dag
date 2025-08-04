#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from Spark_Config_Setup import get_spark
from pyspark.sql import functions as F
spark = get_spark()


# In[ ]:


DF_Gold_Layer = spark.read.format("parquet").option("Header",True) \
    .option("InferSchema",True) \
    .load(r"s3a://supawsbucket07/Airflow_AwSS3_Pyspark_Project/silver/Silver_Layer_Data.parquet")
DF_Gold_Layer.count()
# DF_Gold_Layer.show()


# In[ ]:


DF_Gold_Layer.printSchema()


# In[ ]:


DF_Customer_Fact = DF_Gold_Layer.select(F.col("customer_id"),
        F.col("product_category")).orderBy("customer_id").dropDuplicates()
DF_Customer_Fact.count()


# In[ ]:


DF_Customer_Transaction_Dim = DF_Gold_Layer.select(F.col("transaction_id"),F.col("timestamp").alias("transaction_time"),F.col("customer_id"),F.col("product_category"),F.col("amount_spent"),
F.col("currency")).orderBy("customer_id").dropDuplicates()

DF_Customer_Product_Amount_Dim = DF_Gold_Layer.select(F.col("customer_id"),F.col("product_category"),F.col("total_spent_per_customer_category"),F.col("currency")).orderBy("customer_id").dropDuplicates(["customer_id","product_category"])



# In[ ]:


DF_Customer_Transaction_Dim.count()


# In[ ]:


DF_Customer_Product_Amount_Dim.count()


# In[ ]:


DF_Customer_Fact.write.format("parquet").mode("overwrite") \
    .save(r"s3a://supawsbucket07/Airflow_AwSS3_Pyspark_Project/gold/DF_Customer_Fact.parquet")

DF_Customer_Transaction_Dim.write.format("parquet").mode("overwrite") \
    .save(r"s3a://supawsbucket07/Airflow_AwSS3_Pyspark_Project/gold/DF_Customer_Transaction_Dim.parquet")

DF_Customer_Product_Amount_Dim.write.format("parquet").mode("overwrite") \
    .save(r"s3a://supawsbucket07/Airflow_AwSS3_Pyspark_Project/gold/DF_Customer_Product_Amount_Dim.parquet")


# In[ ]:


# get_ipython().system('jupyter nbconvert --to script /home/airflowsup/Airflow_AwsS3_Project/Airflow_AwsS3_Pyspark_GoldLayerScript.ipynb --output /home/airflowsup/Airflow_AwsS3_Project/Airflow_AwsS3_Pyspark_GoldLayerScript')

