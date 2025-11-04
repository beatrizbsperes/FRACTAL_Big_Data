from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, when, input_file_name
from functools import reduce
import sys
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
import os


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
        self.spark = None
        self.executor_mem = "4g"
        self.driver_mem = "4g"
    
        
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
                             

    def read_parquet(self, bucket_name, path, read_all=False):
        """
        Read the parquet file(s) with inferred schema.
        It accepts str or list of strings as path
        
        Args:
            bucket_name (str): S3 bucket name
            path (str or list of str): Path(s) to parquet file(s) or directory
            read_all (bool): Default False. If True, reads all parquet files in directory
        """
        if read_all:
            self.file_path = f"s3a://{bucket_name}/{path}/*.parquet"
            
        if isinstance(path, str):
            self.file_path = [path]  # Make it a list for uniform handling

        if isinstance(path, list):
            self.file_path = [f"s3a://{bucket_name}/{p}" for p in path]
        
        print(f"Reading from: {self.file_path}")
        
        return self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .parquet(*self.file_path)  # <-- pass the list as *args

    
    def close(self):
        """
        Stop the Spark session and release resources.
        """
        if self.spark:
            self.spark.stop()
            print("\nSpark session stopped.")


class PreProcessing():
    def __init__(self, spark_df):
        self.df = spark_df
    
    def sampling(self, sample_size = 0.1):
        """
        Returns a sample dataset for quick propotype 
        """
        self.df = self.df.sample(fraction=sample_size)
    
    def split_xyz(self):
        """
        Split xyz into three columns on the dataframe
        """
        self.df = self.df.withColumn("x", col("xyz").getItem(0)) \
                        .withColumn("y", col("xyz").getItem(1)) \
                        .withColumn("z", col("xyz").getItem(2))
                        
    def assembler(self, feature_cols):
        """
        Create a Vector Assembler to output the dataframe
        
        """
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        
        return assembler 
        
