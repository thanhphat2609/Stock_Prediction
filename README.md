# Stock Prediction with Hadoop

## Description

This is a simple stock prediction project using technologies such as HDFS, Apache Spark, Apache Hive, and Apache Superset.


## Architecture 




## Installation

1. Clone the repository:

```bash
git clone https://github.com/thanhphatuit/Stock_Prediction.git
```

2. Start Hadoop:

```bash
hdfs namenode -format
start-all.sh
```

3. Create directory:

```
hdfs dfs -mkdir -p /user/thanhphat/datalake
hdfs dfs -mkdir -p /user/hive/warehouse
```


4. Open your browser (Firefox) and go to http://localhost:9870 to interact with the HDFS.

## File Structure

- `spark-ingest.py`: File for Ingestion Data.
- `spark-etl.py`: File for Extract, Transform, Load to Data Warehouse.

Feel free to explore and enhance the project as needed!
