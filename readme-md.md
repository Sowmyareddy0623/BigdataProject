# BlockStock Analytics

## Distributed Analysis of Blockchain and Stock Market Data using Apache Spark

This project demonstrates the power of Apache Spark for big data analytics through the analysis of blockchain and stock market datasets. It showcases various Spark APIs (Spark Core RDD operations, Spark SQL) to extract meaningful insights from large datasets.

## Project Structure

The project is divided into three main components:

1. **Blockchain Data Analysis (Part 1)** - Using Spark Core RDD operations
2. **Stock Market Data Analysis** - Using both Spark Core and Spark SQL
3. **Blockchain Data Analysis (Part 2)** - Using Spark SQL and integration with MySQL

## Dataset Description

### Blockchain Dataset
- Contains information about blockchain blocks including height, timestamp, transaction counts, etc.
- Stored in HDFS as text files in JSON format
- Also available in MySQL database tables (blocks, blocks_info)

### Stock Market Dataset
- Contains stock price data for technology companies (AAPL, IBM, AMZN, META, NFLX)
- Includes timestamp, open, high, low, close prices, and volume
- 5 days of data (2023-10-23 to 2023-10-27)
- Stored in HDFS as CSV files

## Key Analysis Performed

### Blockchain Analysis
- Total number of blocks
- Height of the largest block
- Date and time of the largest block
- Highest number of transactions in a block

### Stock Market Analysis
- Total number of records
- Number of distinct days
- Records per day
- Stock symbols in the dataset
- Highest price for each stock symbol
- Lowest price for each stock symbol
- Average price for each stock symbol
- Price range for each stock symbol
- Date and time of the highest price for each symbol

## Technologies Used

- **Apache Spark**
  - Spark Core (RDD API)
  - Spark SQL
- **Hadoop Distributed File System (HDFS)**
- **MySQL** (for Part 2 of Blockchain analysis)
- **Python** (PySpark)
- **Jupyter Notebook** (for code execution and visualization)

## Prerequisites

- Apache Spark (version 2.x or later)
- Hadoop (HDFS)
- MySQL Server
- Python with PySpark
- Jupyter Notebook

## Setup Instructions

1. **HDFS Setup**
   - Ensure HDFS is running
   - Create directories for your data:
     ```
     hadoop fs -mkdir -p /Neha/1a
     hadoop fs -mkdir -p /Neha/1b
     ```

2. **Data Loading**
   - Load blockchain data to HDFS:
     ```
     hadoop fs -put block_data.txt /Neha/1a/
     ```
   - Load stock market data to HDFS:
     ```
     hadoop fs -put stock_data.csv /Neha/1b/
     ```

3. **MySQL Setup**
   - Create database and tables for blockchain data
   - Import data to MySQL tables

4. **Spark Setup**
   - Ensure Spark is properly installed and configured
   - Download MySQL connector JAR file for Spark-MySQL integration

## Running the Analysis

1. Launch Jupyter Notebook:
   ```
   jupyter notebook
   ```

2. Open the notebook files:
   - `blockchain_analysis_part1.ipynb`
   - `stock_market_analysis.ipynb`
   - `blockchain_analysis_part2.ipynb`

3. Execute the notebook cells to perform the analysis

## Results

### Blockchain Analysis Results
- Total Blocks: 1043
- Height of the largest block: 814345
- Date and time of the largest block: 2023-10-28 23:52:59
- Highest transaction number: 6053

### Stock Market Analysis Results
- Total records: 9750
- Number of distinct days: 5
- Records per day: 1950 records per day
- Stock symbols: NFLX, IBM, AAPL, AMZN, META
- Highest prices:
  - NFLX: 417.09
  - IBM: 144.61
  - AAPL: 174.0
  - AMZN: 129.96
  - META: 318.16

## Future Work

- Implement real-time data processing using Spark Streaming
- Create visualizations using Matplotlib or Tableau
- Perform predictive analysis using Spark MLlib
- Expand the analysis to include more data sources and metrics

## License

This project is licensed under the MIT License - see the LICENSE file for details.
