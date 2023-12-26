# Stock Prediction with Hadoop

## Architecture 

![StockPrediction](https://github.com/thanhphatuit/InternationalLanguageSchool/assets/84914537/9905b074-499f-45b1-96f2-6577956f4031)

## Installation and Step to run project

1. Clone the repository:

```
git clone https://github.com/thanhphatuit/Stock_Prediction.git
cd Code
```

2. Start Hadoop:

```
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

4. Run files:

```
spark-submit Extract_Load.py "executionDate"
spark-submit Transformation.py "executionDate"
```

5. Open your browser (Firefox) and go to http://localhost:9870 to interact with the HDFS.

6. Go to http://localhost:8080 to interact with the Superset.

7. Run and connect Apache Hive.

```
./hive --service hiveserver2 --hiveconf hive.server2.thrift.port=10000 --hiveconf hive.root.logger=INFO,console --hiveconf hive.server2.enable.doAs=false: Start the Apache Hive server.

beeline -u jdbc:hive2://127.0.0.1:10000: Access the Apache Hive command-line.

```

## File Structure

- `Extract_Load.py`: File for Extract and Load raw data to HDFS.
- `Transformation.py`: File for Transformation data from raw data to relational database.
- `LinearRegression.ipynb`: File for Machine Learning to predict stock.

## Video demo
- Link: [Stock_ELT](https://youtu.be/vpJoBiIIxmk).

Feel free to explore and enhance the project as needed!
