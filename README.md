# Warehouse Product Tracking System

## Overview
This project simulates the daily operations of product shipments between multiple warehouse units over a period of 10 days. The Python script computes the new destinations of products based on specific warehouse formulas and conditions. The resulting data is then analyzed using PySpark to provide insights into the shipping operations.

## Features
- Simulates product transfers based on custom business logic.
- Generates a dataset optimized for reporting and analysis.
- Analyzes shipment data using PySpark to determine trends and statistics.

## Setup and Running

### Prerequisites
- Python 3.8+
- Apache Spark 3.0+
- Java 8+

### Installation

1. Clone the repository:
git clone https://github.com/yourusername/HeatTrans.git
cd HeatTrans

2. Create and activate a virtual environment:
python -m venv .venv
.venv\Scripts\activate # On Windows
source .venv/bin/activate # On Unix or MacOS


3. Install the required packages:
pip install -r requirements.txt


4. Set up Hadoop and configure environment variables (if running locally on Windows):
- Download `winutils.exe` compatible with your Spark version.
- Set `HADOOP_HOME` to the directory containing `bin` folder of `winutils.exe`.

### Running the Simulation
To run the Python simulation:
python src/python_assignment.py

To analyze the results with PySpark:
python src/pyspark_assignment.py


