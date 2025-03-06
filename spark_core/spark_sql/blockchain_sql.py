from pyspark.sql import SparkSession
import json

def analyze_blockchain_sql(spark, file_path):
    """
    Analyzes blockchain data using Spark SQL.

    Args:
        spark (SparkSession): The Spark Session.
        file_path (str): Path to the blockchain data file in HDFS.
    """
    try:
        rdd_neha_1a = spark.sparkContext.textFile(file_path)
        myrdd_neha_1a = rdd_neha_1a.map(json.loads)
        df = spark.createDataFrame(myrdd_neha_1a)
        df.createOrReplaceTempView("block_data")

        tot_blck = spark.sql("SELECT COUNT(*) as tot_block FROM block_data").collect()[0].tot_block
        print("1. Total Blocks:", tot_blck)

        l_blk_ht = df.agg({"height": "max"}).collect()[0][0]
        print("2. Height of the largest block:", l_blk_ht)

        lblk_info = df.filter(df.height == l_blk_ht).select("time").collect()[0][0]
        print("3. Largest block Date and Time:", lblk_info)

        high_Tr = df.agg({"n_tx": "max"}).collect()[0][0]
        print("4. Highest transaction number:", high_Tr)

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Initialize Spark Session
    spark = SparkSession.builder.appName("BlockchainDataAnalysisSQL").getOrCreate()

    # HDFS file path
    file_path = "hdfs://localhost/Neha/1a/block_data.txt"

    # Analyze blockchain data
    analyze_blockchain_sql(spark, file_path)

    # Stop Spark Session
    spark.stop()
