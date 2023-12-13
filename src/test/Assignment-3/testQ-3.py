import unittest
from pyspark.sql import SparkSession
from utils import create_log_data_df, calculate_user_actions_last_7_days, convert_timestamp_to_date

class AssignmentTest(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("Test").getOrCreate()
        self.log_data_df = create_log_data_df(self.spark)

    def test_calculate_user_actions_last_7_days(self):
        result = calculate_user_actions_last_7_days(self.log_data_df)
        self.assertEqual(result.count(), 3)  # Replace with the expected count

    def test_convert_timestamp_to_date(self):
        result = convert_timestamp_to_date(self.log_data_df)
        self.assertEqual(result.count(), 8)  # Replace with the expected count

if __name__ == "__main__":
    unittest.main()
