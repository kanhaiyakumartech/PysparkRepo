from pyspark.sql import SparkSession
from src/Assignment-3/utils.py import *
#from utils import process_data, write_csv, write_managed_table

if __name__ == "__main__":
    # Create a Spark session
    spark = SparkSession.builder.appName("Assignment").getOrCreate()

    # Sample data
    data = [
        (1, 101, 'login', '2023-09-05 08:30:00'),
        (2, 102, 'click', '2023-09-06 12:45:00'),
        (3, 101, 'click', '2023-09-07 14:15:00'),
        (4, 103, 'login', '2023-09-08 09:00:00'),
        (5, 102, 'logout', '2023-09-09 17:30:00'),
        (6, 101, 'click', '2023-09-10 11:20:00'),
        (7, 103, 'click', '2023-09-11 10:15:00'),
        (8, 102, 'click', '2023-09-12 13:10:00')
    ]

    # Process data
    df = process_data(spark, data)

    # Write as CSV
    write_csv(df, "dbfs:/FileStore/tables/user.csv")

    # Write as managed table with the correct database name
    write_managed_table(df, "user", "login_details", "overwrite")
    #columns = ["log_id", "user_id", "user_activity", "time_stamp"]
