import os
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Warehouse Product Analysis").getOrCreate()

# Define the path to the data directory, dynamically based on user environment
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
data_file = os.path.join(base_dir, 'daily_operations.csv')

# Load data from CSV
df = spark.read.csv(data_file, header=True, inferSchema=True)

# Show initial DataFrame to check results
df.show()

# Some example analysis
# Aggregate total products shipped to each destination
total_shipments = df.groupBy('Destination').count()
total_shipments.show()

# Filter data for products where the test result was false
false_tests = df.filter(df['Test Result'] == False)
false_tests.show()

# Sort data by new product ID in descending order
sorted_data = df.orderBy(df['New Product ID'].desc())
sorted_data.show()

# Calculate the average new product ID per destination
average_new_product_id = df.groupBy('Destination').avg('New Product ID')
average_new_product_id.show()

# Define output path for analysis results
output_path = os.path.join(base_dir, 'analysis_results')

# Save the analysis results to a file, overwrite if exists
average_new_product_id.write.mode('overwrite').csv(output_path, header=True)

# Stop the Spark session
spark.stop()