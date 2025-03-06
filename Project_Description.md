# Project: Distributed Data Analysis of Blockchain and Stock Market Data Using Apache Spark

## Overview

This project demonstrates the use of Apache Spark for analyzing large datasets related to blockchain and stock market data. It leverages both Spark Core and Spark SQL to perform various data analysis tasks, extracting valuable insights from these datasets. The project is structured into four main parts:

*   **Part 1 & 3:** Blockchain Data Analysis using Spark Core and Spark SQL
*   **Part 2 & 4:** Stock Market Data Analysis using Spark Core and Spark SQL

## Data Sources

*   **Blockchain Data:**  Stored in HDFS in text format (`block_data.txt` and data from Hive tables).
*   **Stock Market Data:**  Stored in HDFS in CSV format.

## Key Objectives

The primary objectives of this project are:

*   To perform data analysis on blockchain data to extract key metrics such as total blocks, largest block height, and the highest number of transactions in a block.
*   To perform data analysis on stock market data to calculate statistics such as total records, distinct days, stock symbols, highest/lowest/average prices for each symbol, and price ranges.
*   To demonstrate proficiency in using both Spark Core and Spark SQL for data processing and analysis.

## Project Structure

The project is divided into the following modules:

1.  **Blockchain Data Analysis (Spark Core)**
2.  **Stock Market Data Analysis (Spark Core)**
3.  **Blockchain Data Analysis (Spark SQL)**
4.  **Stock Market Data Analysis (Spark SQL)**

Each module contains the necessary Python scripts and data to perform the analysis.

## Technologies Used

*   Apache Spark
*   Hadoop (HDFS)
*   Python 3.x
*   PySpark
*   Spark SQL

## Instructions for Setup and Execution

1.  Ensure that Apache Spark and Hadoop are properly installed and configured.
2.  Place the data files in the appropriate HDFS directories.
3.  Use `spark-submit` to run the Python scripts.

## Key Findings

*   Analysis of blockchain data revealed insights into the structure and transaction activity of blockchain networks.
*   Analysis of stock market data provided statistics on stock prices, trading volumes, and price ranges.

## Future Enhancements

*   Implement more advanced data analysis techniques.
*   Visualize the results using data visualization libraries.
*   Develop a user interface for easier interaction.



