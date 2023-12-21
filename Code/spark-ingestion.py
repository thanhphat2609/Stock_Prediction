
# ========================================================================== STEP 1: CONFIG SPARK ==========================================================================

import sys

from pyspark import SparkContext, SparkConf, SparkFiles
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.functions import lit, col, when, row_number, split
from pyspark.sql.window import Window

# Create Spark Session	
spark = SparkSession.builder.appName("SparkIngestion").getOrCreate()

# Receive argument
executionDate = sys.argv[1].split("/")
year = executionDate[0]
month = executionDate[1]
date = executionDate[2]


# ================================================================================= STEP 2: READ DATA =================================================================================
def read_data_csv(url, file_name):
	spark.sparkContext.addFile(url)
	dataframe = spark.read.csv(SparkFiles.get(file_name), inferSchema = True, header = True)
	return dataframe


url = 'https://raw.githubusercontent.com/anhthu04091511/VCB-stock-price-prediction/main/dataset/data_chungkhoan_VCB.csv'
file_name = 'data_chungkhoan_VCB.csv'


df_vcb = read_data_csv(url, file_name)



# ====================================================================== STEP 3: LOAD DATA(PARTITION) TO HDFS ==========================================================================

df_vcb_parition = df_vcb.withColumn("year", lit(year)).withColumn("month", lit(month)).withColumn("date", lit(date))

df_vcb_parition.write.partitionBy("year", "month", "date").mode("overwrite").csv("hdfs://localhost:9000/user/thanhphat/datalake/", header = True)




# ========================================================================== STEP 5: STOP SPARK SESSION ==========================================================================
spark.stop
