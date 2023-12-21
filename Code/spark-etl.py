# ====================================================================== STEP 1: EXTRACT DATA FROM HDFS ======================================================================

from pyspark import SparkContext, SparkConf
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.functions import lit, col, when, row_number, split
from pyspark.sql.window import Window

# Connect to Hive	
spark = SparkSession.builder.appName("SparkELT").config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse").enableHiveSupport().getOrCreate()

def read_csv(input_path):
	dataframe = spark.read.csv(input_path, header = True, inferSchema = True)
	return dataframe

# Receive argument
executionDate = sys.argv[1].split("/")
year = executionDate[0]
month = executionDate[1]
date = executionDate[2]

input_path = 'hdfs://localhost:9000/user/thanhphat/datalake/year={}/month={}/date={}/part-*.csv'.format(year, month, date)

df_vcb = read_csv(input_path)
df_vcb = df_vcb.drop("year", "month", "day")

# ============================================================================== STEP 2: TRANSFORM DATA =================================================================================

# Funtion change column
def choose_column(dataframe, select_cols):
	# Select all columns in list
	dataframe_new = dataframe.select(*select_cols)
	return dataframe_new

# Function change column name
def change_column_name(dataframe, new_column_name):
	dataframe_new = dataframe.toDF(*new_column_name) 
	return dataframe_new	
	
# Function sort data by year	
def sort_year(dataframe):
	split_date = split(dataframe['Date'], '/')

	dataframe = dataframe.withColumn('Year', split_date.getItem(2))

	dataframe_new = dataframe.sort(df_vcb.Year.asc()).drop("Year")
	
	return dataframe_new


# Define list of columns to select
select_cols = ['Ngày', 'Giá điều chỉnh', 'Giá đóng cửa', 'Giá mở cửa', 'Giá cao nhất', 'Giá thấp nhất']
df_vcb = choose_column(df_vcb, select_cols)

# Define new column_name
new_column_name = ['Date', 'Ajusting Price', 'Closing Price', 'Opening Price', 'High Price', 'Low Price']
df_vcb = change_column_name(df_vcb, new_column_name)

# Sort data
df_vcb = sort_year(df_vcb)


# ============================================================================== STEP 3: LOAD DATA =================================================================================

# Function create database
def create_database(database_name):
	spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name};")
	spark.sql(f"USE {database_name}")

def create_tabledata(dataframe, table_name):
	dataframe.write.mode('overwrite').saveAsTable(f"{table_name}")
	
	
# Create database
database_name = 'Stoc_VCB'
create_database(database_name)

# Create table with dataframe	
table_name = 'TBL_VCB_STOCK'
create_tabledata(df_vcb, table_name)


# ========================================================================== STEP 4: STOP SPARK SESSION ==========================================================================
spark.stop
