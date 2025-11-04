from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, when, input_file_name
from functools import reduce
import sys
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
import os
from pyspark.sql import Window
from pyspark.sql.functions import (
    col, sqrt, mean, stddev, count, 
    min as spark_min, max as spark_max
)

## Note
## This is slightly different from Sparker2.py because there are two methods
## to create the session, locally and on the bucket, this is more for a 
## single file exploration but running on a jupyter notebook

class Sparker:
    """
    A class to handle Spark operations on S3 parquet files.
    """
    
    def __init__(self, access_key, secret_key, run_locally=False):
        """
        Initialize Sparker with S3 credentials and file information.
        
        Args:
            access_key (str): AWS access key
            secret_key (str): AWS secret key
        """
        self.access_key = access_key
        self.secret_key = secret_key
        
        self.spark = None
        
  
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
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.connection.timeout", "50000") \
            .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000") \
            .config("spark.hadoop.fs.s3a.multipart.purge.age", "30000000") \
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        
        self.spark = spark
    
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
                             
    # def read_parquet(self, bucket_name, path, read_all=True):
    #     """
    #     Read the parquet file(s) with inferred schema.
    
    #     Args:
    #         bucket_name (str): S3 bucket name
    #         path (str): Path to parquet file or directory
    #         read_all (bool) True: If True, reads all parquet files in directory
    #     """
        
    #     # Construct full S3 path
    #     if read_all:
    #         self.file_path = f"s3a://{bucket_name}/{path}/*.parquet"  ## catch all files in a bucket
    #     else:
    #         self.file_path = f"s3a://{bucket_name}/{path}"
            
    #     print(f"Reading from: {self.file_path}")
        
    #     return self.spark.read \
    #             .option("header", "true") \
    #             .option("inferSchema", "true") \
    #             .parquet(self.file_path)


    def read_parquet(self, bucket_name, path, read_all=True):
        """
        Read the parquet file(s) with inferred schema.
        
        Args:
            bucket_name (str): S3 bucket name
            path (str or list of str): Path(s) to parquet file(s) or directory
            read_all (bool): If True, reads all parquet files in directory
        """
        
        if isinstance(path, str):
            path = [path]  # Make it a list for uniform handling

        paths_to_read = []
        for p in path:
            if read_all:
                paths_to_read.append(f"s3a://{bucket_name}/{p}/*.parquet")
            else:
                paths_to_read.append(f"s3a://{bucket_name}/{p}")
        
        print(f"Reading from: {paths_to_read}")
        
        return self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .parquet(*paths_to_read)  # <-- pass the list as *args

    
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

class FeatureEngineering():
    def __init__(self, spark_df):
        self.df = spark_df
    
    def height_above_ground(self, grid_size=5.0):
        """
        Calculate height above local ground.
        """
        self.df = self.df.withColumn("gx", (col("x") / grid_size).cast("int")) \
                         .withColumn("gy", (col("y") / grid_size).cast("int"))
        
        window = Window.partitionBy("gx", "gy")
        self.df = self.df.withColumn("ground_z", spark_min("z").over(window))
        
        self.df = self.df.withColumn("height_above_ground", col("z") - col("ground_z"))
        self.df = self.df.drop("gx", "gy", "ground_z")
        return self.df
    
    def local_stats(self, grid_size=2.0):
        """
        Calculate local neighborhood statistics.
        Creates roughness and density features.
        """
        self.df = self.df.withColumn("lx", (col("x") / grid_size).cast("int")) \
                         .withColumn("ly", (col("y") / grid_size).cast("int"))
        
        window = Window.partitionBy("lx", "ly")
        
        # local statistics
        self.df = self.df.withColumn("local_density", count("*").over(window)) \
                         .withColumn("local_z_std", stddev("z").over(window)) \
                         .withColumn("local_z_range", 
                                   spark_max("z").over(window) - spark_min("z").over(window))
        
        # roughness (normalized std)
        self.df = self.df.withColumn("roughness", 
                                    col("local_z_std") / (col("local_z_range") + 0.01))
        
        self.df = self.df.drop("lx", "ly")
        return self.df
    
    def return_features(self):
        """
        Features from LiDAR returns.
        Key for vegetation vs building classification.
        """
        self.df = self.df.withColumn("return_ratio", 
                                    col("ReturnNumber") / col("NumberOfReturns")) \
                         .withColumn("is_single_return", 
                                   (col("NumberOfReturns") == 1).cast("int")) \
                         .withColumn("is_last_return",
                                   (col("ReturnNumber") == col("NumberOfReturns")).cast("int"))
        return self.df
    
    def vegetation_index(self):
        """
        NDVI for vegetation detection.
        Green red ratio.
        """
        self.df = self.df.withColumn("ndvi", (col("Infrared") - col("Red")) / (col("Infrared") + col("Red") + 0.001))
        self.df = self.df.withColumn("green_red_ratio", col("Green") / (col("Red") + 0.001))
        return self.df
    
    def weater_detection(self):
        """
        NDWI for water detection
        """
        self.df = self.df.withColumn("ndwi", (col("Green") - col("Infrared")) / (col("Green") + col("Infrared") + 0.001))
        return self.df

