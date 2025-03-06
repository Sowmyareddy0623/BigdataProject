from pyspark import SparkContext
import json
import datetime

def analyze_blockchain_core(sc, file_path):
    """
    Analyzes blockchain data using Spark Core.

    Args:
        sc (SparkContext): The Spark Context.
        file_path (str): Path to the blockchain data file in HDFS.
    """
    try:
        myrdd1 = sc.textFile(file_path)
        rdd1 = myrdd1.map(json.loads)

        total_blocks = rdd1.count()
        print('1. Total number of Blocks:', total_blocks)

        larg_blk_ht = rdd1.max(lambda x: x["height"])
        print('2. Height of largest block:', larg_blk_ht['height'])

        rdd_3 = rdd1.filter(lambda x: x["height"] == larg_blk_ht['height'])
        dateandtime_largeblock = rdd_3.map(lambda x: x["time"]).first()
        dt_object = datetime.datetime.fromtimestamp(dateandtime_largeblock)
        print("3. Largest block's date and time:", dt_object.strftime('%Y-%m-%d %H:%M:%S'))

        tx_cnt = rdd1.map(lambda x: x["n_tx"]).max()
        print("4. Highest number of transactions:", tx_cnt)

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Initialize Spark Context
    sc = SparkContext("local", "BlockchainAnalysis")

    # HDFS file path
    file_path = "hdfs://localhost/Neha/1a/block_data.txt"

    # Analyze blockchain data
    analyze_blockchain_core(sc, file_path)

    # Stop Spark Context
    sc.stop()
