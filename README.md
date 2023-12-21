# Stock Prediction with Hadoop

## Description

This is a simple stock prediction project using technologies such as HDFS, Apache Spark, Apache Hive, and Apache Superset.


## Architecture 

![Stock_Prediction](https://github.com/thanhphatuit/Stock_Prediction/assets/84914537/a4856130-cd5f-4bf0-974a-490dbd7c9fff)

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

4. Step run:

```
spark-submit spark-ingest.py
spark-submit spark-etl.py
spark-submit spark-ml.py
```

5. Open your browser (Firefox) and go to http://localhost:9870 to interact with the HDFS.

## File Structure

- `spark-ingest.py`: File for Ingestion Data.
- `spark-etl.py`: File for Extract, Transform, Load to Data Warehouse.
- `spark-ml.py`: File for Machine Learning to predict stock.

## Video demo
- Ingest: .
- ETL: .
- Machine Learning: .

Feel free to explore and enhance the project as needed!
