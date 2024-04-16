import os
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("Warehouse Product Analysis").getOrCreate()

# Define the path to the data directory, dynamically based on user environment
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
data_file = os.path.join(base_dir, 'daily_operations.csv')

# Load data from CSV
df = spark.read.csv(data_file, header=True, inferSchema=True)

# Show initial DataFrame to check results
df.show()

# Some example analysis and visualization
# Aggregate total products shipped to each destination
total_shipments = df.groupBy('product_destination').count()
total_shipments.show()

# Convert Spark DataFrame to Pandas DataFrame for plotting
total_shipments_pd = total_shipments.toPandas()

# Plotting total shipments to each destination
plt.figure(figsize=(10, 6))
plt.bar(total_shipments_pd['product_destination'].astype(str), total_shipments_pd['count'], color='blue')
plt.xlabel('Product Destination')
plt.ylabel('Total Shipments')
plt.title('Total Products Shipped to Each Destination')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Stop the Spark session
spark.stop()
