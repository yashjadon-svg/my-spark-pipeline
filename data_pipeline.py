# data_pipeline.py - Version 1

from pyspark.sql import SparkSession

def create_spark_session():
    """Initializes and returns a SparkSession."""
    spark = (
        SparkSession.builder
        .appName("InitialSparkPipeline")
        .getOrCreate()
    )
    print("SparkSession created successfully!")
    return spark

def main():
    """Main function to run the data pipeline."""
    spark = create_spark_session()
    spark.stop()
    print("SparkSession stopped.")

if __name__ == "__main__":
    main()
