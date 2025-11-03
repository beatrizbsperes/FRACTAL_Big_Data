from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from functools import reduce
from pyspark.sql.functions import col, sum as spark_sum, when

class Sparker:
    """
    A class to handle Spark operations on S3 parquet files.
    """
    
    def __init__(self, access_key, secret_key, bucket_name, file_name):
        """
        Initialize Sparker with S3 credentials and file information.
        
        Args:
            access_key (str): AWS access key
            secret_key (str): AWS secret key
            bucket_name (str): S3 bucket name
            file_name (str): Path to the parquet file within the bucket
        """
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket_name = bucket_name
        self.file_name = file_name
        
        # Construct full S3 path
        self.file_path = f"s3a://{bucket_name}/{file_name}"
        
        # Initialize Spark Session
        self.spark = self._create_session()
        
        # Read the parquet file and infer schema
        self.df = None
        self._read_parquet()
    
    def _create_session(self):
        """
        Create and configure Spark session with S3 settings.
        
        Returns:
            SparkSession: Configured Spark session
        """
        spark = SparkSession.builder \
            .appName("Sparker - S3 Parquet Reader") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.access.key", self.access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", self.secret_key) \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.connection.timeout", "50000") \
            .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000") \
            .config("spark.hadoop.fs.s3a.multipart.purge.age", "30000000") \
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000") \
            .getOrCreate()

        # Set log level to WARN to reduce output noise
        #spark.sparkContext.setLogLevel("WARN")
        spark.sparkContext.setLogLevel("ERROR")
        
        return spark
    
    def _read_parquet(self):
        """
        Read the parquet file with inferred schema and print information.
        """
        print(f"Reading file: {self.file_path}")
        
        # Read parquet with schema inference
        self.df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .parquet(self.file_path)
        
        # Print basic information
        print(f"\nTotal rows: {self.df.count()}")
        print(f"\nInferred Schema:")
        self.df.printSchema()
    
    def close(self):
        """
        Stop the Spark session and release resources.
        """
        if self.spark:
            self.spark.stop()
            print("\nSpark session stopped.")
            

    # def _count_rows_with_nulls(df):
    #     conditions = [col(c).isNull() for c in df.columns]
    #     combined_condition = reduce(lambda a, b: a | b, conditions)
    #     return df.filter(combined_condition).count()
    
    # def _count_nulls_per_column(df):
    #     # Returns a dictionary with column names and their null counts
    #     null_counts = df.select([
    #         spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    #         for c in df.columns
    #     ]).collect()[0]
        
    #     return null_counts.asDict()
    
# Main execution
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 5:
        print("Usage: spark-submit --master local[4] --packages org.apache.hadoop:hadoop-aws:3.3.1 sparker.py <access_key> <secret_key> <bucket_name> <file_path>")
        print("\nExample:")
        print("  spark-submit --master local[4] --packages org.apache.hadoop:hadoop-aws:3.3.1 sparker.py ACCESS_KEY SECRET_KEY ubs-datasets FRACTAL/data/TRAIN-1200_6136-008972557.parquet")
        sys.exit(1)
    
    access_key = sys.argv[1]
    secret_key = sys.argv[2]
    bucket_name = sys.argv[3]
    file_name = sys.argv[4]
    
    # Create Sparker instance
    sparker = Sparker(access_key, secret_key, bucket_name, file_name)
    
    ## ANY FUNCTION HERE
    df = sparker.df
    conditions = [col(c).isNull() for c in df.columns]
    combined_condition = reduce(lambda a, b: a | b, conditions)
    print(f"Rows with at least one null value: {df.filter(combined_condition).count()}")
    
    null_counts = df.select([
            spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
            for c in df.columns
        ]).collect()[0]
    
    # If using option 4:
    print(f"Null counts per column: {null_counts}")
    
    # Close session when done
    sparker.close()