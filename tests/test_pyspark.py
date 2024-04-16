import sys
import os
import unittest
from pyspark.sql import SparkSession

# Adjust the path to import pyspark_assignment
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

from pyspark_assignment import transform_data

class PySparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .master("local[2]") \
            .appName("WarehouseProductAnalysisTest") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_data_processing(self):
        # Example data for testing
        input_data = [("0", "Warehouse 0", "multiply by 7", "6", "74", "518", "false", "1"),
                      ("1", "Warehouse 1", "square the number", "5", "69", "4761", "false", "1")]
        schema = ["warehouse_id", "warehouse_name", "warehouse_formula", "product_destination", "starting_product_id", "current_product_id", "test_bool", "day_number"]
        df = self.spark.createDataFrame(input_data, schema=schema)
        
        # Perform some transformations or actions
        result_df = transform_data(df) 

        # Test the results
        self.assertEqual(result_df.count(), 2)
        self.assertEqual(result_df.filter(result_df['warehouse_id'] == "0").count(), 1)

if __name__ == '__main__':
    unittest.main()
