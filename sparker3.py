from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, when, input_file_name, isnull
from functools import reduce
import sys
from pyspark.ml import Pipeline
from pyspark.sql import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
import os

## Note
## This is slightly different from Sparker2.py because there are two methods
## to create the session, locally and on the bucket, this is more for a 
## single file exploration but running on a jupyter notebook

class Sparker:
    """
    A class to handle Spark operations on S3 parquet files.
    """
    
    def __init__(self, access_key=None, secret_key=None):
        """
        Initialize Sparker with S3 credentials and file information.
        Access key and Secret key are only necessary when not running inside the AWS EC2 cluster.
        
        Args:
            access_key (str): AWS access key
            secret_key (str): AWS secret key
        """
        self.access_key = access_key
        self.secret_key = secret_key
        self.executor_mem = "4g"
        self.driver_mem = "4g"
        self.spark = None
        
    def _create_on_cluster_session(self):
        spark =  ( 
            SparkSession.builder 
                .appName("Read FRACTAL files") 
                .config("spark.hadoop.fs.s3a.fast.upload", "true")
                .config("spark.hadoop.fs.s3a.multipart.size", "104857600")
                .config("spark.executor.memory",self.executor_mem)
                .config("spark.driver.memory", self.driver_mem)
                .getOrCreate()
            )
        self.spark = spark
        
        print("Session Created!")
        print(f"- executor memory= {self.executor_mem}")
        print(f"- driver memory= {self.driver_mem}")
        
    
    def _create_local_session(self):
        """
        Try to create a local session to run in a notebook
        """
        spark = SparkSession.builder \
                .appName("Local Session my friend") \
                .master("local[4]") \
                .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.access.key", os.environ['ACCESS_KEY']) \
                .config("spark.hadoop.fs.s3a.secret.key", os.environ['ACCESS_SECRET']) \
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
                .config("spark.hadoop.fs.s3a.connection.timeout", "50000") \
                .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000") \
                .config("spark.hadoop.fs.s3a.multipart.purge.age", "30000000") \
                .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000") \
                .getOrCreate()
                
        spark.sparkContext.setLogLevel("ERROR")
        self.spark = spark
        
        return spark
                             
    def read_parquet(self, bucket_name, path, read_all=True):
        """
        Read the parquet file(s) with inferred schema.
    
        Args:
            bucket_name (str): S3 bucket name
            path (str): Path to parquet file or directory
            read_all (bool) True: If True, reads all parquet files in directory
        """
        
        # Construct full S3 path
        if read_all:
            self.file_path = f"s3a://{bucket_name}/{path}/*.parquet"  ## catch all files in a bucket
        else:
            self.file_path = f"s3a://{bucket_name}/{path}"
        
        print("-"*30)
        print(f"Reading from: {self.file_path}")
        
        return self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .parquet(self.file_path)
    
    def close(self):
        """
        Stop the Spark session and release resources.
        """
        if self.spark:
            self.spark.stop()
            print("\nSpark session stopped.")


# Main execution
if __name__ == "__main__":
    
    # Create Sparker instance
    sparker = Sparker()
    
    ## Create Session 
    sparker._create_on_cluster_session()
    
    ## Read the parquet function 
    df = sparker.read_parquet("ubs-datasets",
                              "FRACTAL/data/test/TEST-1176_6137-009200000.parquet",
                              read_all=False)
    
    # Convert to RDD and check for nulls in each row
    def has_nulls_in_row(row):
        """Check if a row has any null values"""
        return any(value is None for value in row)

    # Map each row to check for nulls
    rows_with_nulls = df.rdd.filter(has_nulls_in_row)

    # Count rows with nulls
    null_row_count = rows_with_nulls.count()
    total_rows = df.rdd.count()

    print(f"Rows with nulls: {null_row_count}/{total_rows}")
    
    # Close session when done
    sparker.close()
        
