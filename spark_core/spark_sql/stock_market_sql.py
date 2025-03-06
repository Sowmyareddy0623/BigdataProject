from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, max as sf_max, min as sf_min, avg as sf_avg
import json

def analyze_stock_market_sql(spark, file_path):
    """
    Analyzes stock market data using Spark SQL.

    Args:
        spark (SparkSession): The Spark Session.
        file_path (str): Path to the stock market data file in HDFS.
    """
    try:
        myrdd_neha_1b = spark.sparkContext.wholeTextFiles(file_path).values()
        myrdd_list = myrdd_neha_1b.collect()

        stock_df = spark.sparkContext.parallelize(myrdd_list) \
            .flatMap(lambda line: line.strip().split('\n')[1:]) \
            .map(lambda line: line.split(',')) \
            .map(lambda fields: json.dumps({
                "Stock": fields[0],
                "Timestamp": fields[1],
                "Open": int(fields[2]),
                "High": float(fields[3]),
                "Low": float(fields[4]),
                "Close": float(fields[5]),
                "Volume": float(fields[6])
            }))

        stocks_df = stock_df.map(json.loads)
        df = spark.createDataFrame(stocks_df)
        df.createOrReplaceTempView('stock_data')

        record_count = spark.sql("SELECT COUNT(*) as record_count FROM stock_data").collect()[0].record_count
        print("1. Total number of records:", record_count)

        days = spark.sql("SELECT COUNT(DISTINCT SUBSTRING(`Timestamp`, 1, 10)) as days FROM stock_data").collect()[0].days
        print("2. Number of different days:", days)

        daily_records = df.groupBy(substring("Timestamp", 1, 10).alias("day")).count()
        print("3. Number of Records per each day:")
        daily_records.show()

        stock_symbol = spark.sql("SELECT DISTINCT Stock FROM stock_data").collect()
        print("4. Stock symbols in the table:")
        for row in stock_symbol:
            print(row.Stock)

        high_price = spark.sql("SELECT Stock, MAX(High) as highest_price FROM stock_data GROUP BY Stock").collect()
        print("5. Highest price for each symbol:")
        for row in high_price:
            print(row.Stock, row.highest_price)

        low_price = spark.sql("SELECT Stock, MIN(Low) as lowest_price FROM stock_data GROUP BY Stock").collect()
        print("6. Lowest price for each symbol:")
        for row in low_price:
            print(row.Stock, row.lowest_price)

        average_price = spark.sql("SELECT Stock, AVG(Close) as average_price FROM stock_data GROUP BY Stock").collect()
        print("7. Average price for each symbol:")
        for row in average_price:
            print(row.Stock, row.average_price)

        price_range = spark.sql("SELECT Stock, MAX(High) - MIN(Low) as price_range FROM stock_data GROUP BY Stock").collect()
        print("8. Price range for each symbol:")
        for row in price_range:
            print(row.Stock, row.price_range)

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Initialize Spark Session
    spark = SparkSession.builder.appName("StockMarketDataAnalysisSQL").getOrCreate()

    # HDFS file path
    file_path = "hdfs://localhost/Neha/1b"

    # Analyze stock market data
    analyze_stock_market_sql(spark, file_path)

    # Stop Spark Session
    spark.stop()
