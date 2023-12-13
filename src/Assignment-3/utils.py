from pyspark.sql import functions as F
from pyspark.sql.types import DateType


def process_data(spark, data):
    # Create DataFrame with custom schema
    columns = ["log_id", "user_id", "user_activity", "time_stamp"]
    df = spark.createDataFrame(data, columns)

    # Query to calculate the number of actions performed by each user in the last 7 days
    last_7_days_df = df.filter((F.current_date() - F.to_date("time_stamp")).cast("int") <= 7)
    actions_by_user = last_7_days_df.groupBy("user_id").agg(F.count("log_id").alias("actions_last_7_days"))

    # Convert timestamp to login_date with yyyy-MM-dd format
    df = df.withColumn("login_date", F.to_date("time_stamp").cast(DateType())).drop("time_stamp")

    # Join the original DataFrame with the calculated actions
    df = df.join(actions_by_user, "user_id", "left_outer")

    return df

def write_csv(df, output_path):
    # Write DataFrame as CSV file
    df.write.option("header", "true").mode("overwrite").csv(output_path)

# def write_csv(df, output_path):
#     # Write DataFrame as CSV file
#     df.write.option("csv")("header","true").mode("overwrite").csv("dbfs:/FileStore/tables/user.csv").save()
#     #df.write.option("csv")("header", "true").mode("overwrite").csv(output_path)

def write_managed_table(df, database, table, mode):
    # Create the database if it doesn't exist
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

    # Use the specified database
    spark.sql(f"USE {database}")

    # Write DataFrame as managed table
    df.write.format("parquet").mode(mode).saveAsTable(table)
