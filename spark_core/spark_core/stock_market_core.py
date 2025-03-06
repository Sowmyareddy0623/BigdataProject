from pyspark import SparkContext
import json

def analyze_stock_market_core(sc, file_path):
    """
    Analyzes stock market data using Spark Core.

    Args:
        sc (SparkContext): The Spark Context.
        file_path (str): Path to the stock market data file in HDFS.
    """
    try:
        rdd_B = sc.textFile(file_path)

        # Parse and transform the data
        stock_rdd = rdd_B.map(lambda line: line.split(',')) \
            .filter(lambda fields: len(fields) == 7) \
            .map(lambda fields: {
                "Stock": fields[0],
                "Timestamp": fields[1],
                "Open": int(fields[2]),
                "High": float(fields[3]),
                "Low": float(fields[4]),
                "Close": float(fields[5]),
                "Volume": float(fields[6])
            })

        print('1. Total no of records:', stock_rdd.count())

        days = stock_rdd.map(lambda x: x["Timestamp"][:10]).distinct().count()
        print("2. No of distinct days:", days)

        records = stock_rdd.map(lambda x: (x["Timestamp"][:10], 1)).reduceByKey(lambda x, y: x + y)
        print("3. Number of Records per each day:")
        for record in records.collect():
            print(f"{record[0]}: {record[1]}")

        symbols = stock_rdd.map(lambda x: x["Stock"]).distinct()
        print("4. symbols:", symbols.collect())

        def calculate_stats(values):
            high_prices = [v["High"] for v in values]
            low_prices = [v["Low"] for v in values]
            close_prices = [v["Close"] for v in values]

            max_price = max(high_prices)
            min_price = min(low_prices)
            avg_price = sum(close_prices) / len(close_prices)
            range_price = max_price - min_price

            return {
                "max_price": max_price,
                "min_price": min_price,
                "avg_price": avg_price,
                "range_price": range_price
            }

        que5 = stock_rdd.groupBy(lambda x: x["Stock"]).mapValues(calculate_stats)

        print("5. Highest price for each symbol:")
        for symbol, stats in que5.collect():
            print(f"{symbol}: {stats['max_price']}")

        print("6. Lowest price for each symbol:")
        for symbol, stats in que5.collect():
            print(f"{symbol}: {stats['min_price']}")

        print("7. Average price for each symbol:")
        for symbol, stats in que5.collect():
            print(f"{symbol}: {stats['avg_price']}")

        print("8. Range of price for each symbol:")
        for symbol, stats in que5.collect():
            print(f"{symbol}: {stats['range_price']}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Initialize Spark Context
    sc = SparkContext("local", "StockMarketAnalysis")

    # HDFS file path
    file_path = "hdfs://localhost/Neha/1b"

    # Analyze stock market data
    analyze_stock_market_core(sc, file_path)

    # Stop Spark Context
    sc.stop()
