#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from Spark_Config_Setup import get_spark
from pyspark.sql import functions as F
from pyspark.sql import Window as W
spark = get_spark()


# In[ ]:


DF_Silver_Layer = spark.read.format("parquet") \
    .option("Header",True) \
    .option("InferSchema",True) \
    .load(r"s3a://supawsbucket07/Airflow_AwSS3_Pyspark_Project/bronze/rejects/Bronze_Layer_Data.parquet")
# DF_Silver_Layer.count()


# In[ ]:


Conversion_Rate_For_Euro = 1.158
Conversion_Rate_For_INR =  0.011
DF_Silver_Layer_Temp = DF_Silver_Layer. \
    withColumn("amount_spent" ,  F.when(F.col("currency") == "EUR",F.round(F.col ("amount_spent") * Conversion_Rate_For_Euro,2)) \
    .when(F.col("currency") == "INR", F.round(F.col("amount_spent") * Conversion_Rate_For_INR,2)) \
    .otherwise(F.round(F.col("amount_spent"),2))) \
    .withColumn("currency", F.lit("USD")) \
    .withColumn("timestamp",F.to_date(F.col("timestamp")))

Customer_Category_Window = W.partitionBy("customer_id","product_category")
DF_Silver_Layer_Temp = DF_Silver_Layer_Temp.withColumn("total_spent_per_customer_category",
                                    F.round(F.sum("amount_spent").over(Customer_Category_Window),2))
# DF_Silver_Layer_Temp.count()
                   
# DF_Silver_Layer_Temp.show()


# In[ ]:


DF_Silver_Layer_Temp.write.format("parquet").mode("overwrite") \
    .save(r"s3a://supawsbucket07/Airflow_AwSS3_Pyspark_Project/silver/Silver_Layer_Data.parquet")


# In[ ]:


# get_ipython().system('jupyter nbconvert --to script /home/airflowsup/Airflow_AwsS3_Project/Airflow_AwsS3_Pyspark_SilverLayerScript.ipynb --output /home/airflowsup/Airflow_AwsS3_Project/Airflow_AwsS3_Pyspark_SilverLayerScript')

