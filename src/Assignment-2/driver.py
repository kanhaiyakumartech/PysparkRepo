from pyspark.sql import SparkSession
from utils import mask_card_udf

# Here I  Create a SparkSession
spark = SparkSession.builder.appName("CreditCardData").getOrCreate()

# Now I Define the dataset
data = [
    ("1234567891234567",),
    ("5678912345671234",),
    ("9123456712345678",),
    ("1234567812341122",),
    ("1234567812341342",)
]

# Define the schema for the DataFrame
schema = ["card_number"]

from src/Assignment-2/utils.py import *
# Create DataFrame using createDataFrame method
credit_card_df = spark.createDataFrame(data, schema)

# Show the DataFrame
credit_card_df.show()

# Get the number of partitions
num_partitions = credit_card_df.rdd.getNumPartitions()
print("Number of partitions:", num_partitions)

# Repartition the DataFrame
repartitioned_df = credit_card_df.repartition(13)
repartitioned_partitions = repartitioned_df.rdd.getNumPartitions()
print("Number of repartitioned partitions:", repartitioned_partitions)

# Reduce the partition size back to its original size
back_to_og_credit_card_df = repartitioned_df.coalesce(num_partitions)
back_num_partitions = back_to_og_credit_card_df.rdd.getNumPartitions()
print("Number of back_to_og_partitions:", back_num_partitions)

# Apply the UDF to create a new column 'masked_card_number'
masked_credit_card_df = back_to_og_credit_card_df.withColumn("masked_card_number", mask_card_udf()("card_number"))

# Show the DataFrame with masked card numbers and repartitioned data
print("DataFrame with masked card numbers and repartitioned data:")
masked_credit_card_df.show()
