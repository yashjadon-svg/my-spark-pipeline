# data_pipeline.py - Version 2 (on feature/read-csv-data branch)

from pyspark.sql import SparkSession

def create_spark_session():
    # ... (code is the same as before) ...
    pass

def read_data(spark, file_path):
    """Reads data from a specified file path."""
    print(f"Reading data from {file_path}...")
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    return df

def main():
    """Main function to run the data pipeline."""
    spark = create_spark_session()
    
    # Use the new function to read some data
    data_df = read_data(spark, "s3://my-bucket/raw_data.csv")
    data_df.printSchema()
    
    spark.stop()
    print("SparkSession stopped.")

if __name__ == "__main__":
    main()

