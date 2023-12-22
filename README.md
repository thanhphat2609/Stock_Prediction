# Stock Prediction with Hadoop

## Description

This is a simple stock prediction project using technologies such as HDFS, Apache Spark, Apache Hive, and Apache Superset.


## Architecture 

![StockPrediction](https://github.com/thanhphatuit/InternationalLanguageSchool/assets/84914537/9905b074-499f-45b1-96f2-6577956f4031)

## Installation

1. Clone the repository:

```bash
git clone https://github.com/thanhphatuit/Stock_Prediction.git
cd Code
```

2. Start Hadoop:

```bash
hdfs namenode -format
start-all.sh
```

3. Create directory:

```
hdfs dfs -mkdir -p /user/thanhphat/datalake
hdfs dfs -chmod g+w /user/thanhphat/datalake

hdfs dfs -mkdir -p /user/hive/warehouse
hdfs dfs -chmod g+w /user/hive/warehouse
```

4. Step run:

```
spark-submit spark-ingestion.py "executionDate"
spark-submit spark-transformation.py "executionDate"
```

5. Open your browser (Firefox) and go to http://localhost:9870 to interact with the HDFS.

## File Structure

- `spark-ingest.py`: File for Extract, Load Data to HDFS.
- `spark-transformation.py`: File for Transformation to Data Warehouse.
- `spark-ml.ipynb`: File for Machine Learning to predict stock.

## Video demo
- Ingest: https://youtu.be/cs7IKZtwrK8.
- ETL: .
- Machine Learning: .

Feel free to explore and enhance the project as needed!
