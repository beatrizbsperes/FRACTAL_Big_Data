from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, when, input_file_name
from functools import reduce
import sys
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from sparkmeasure import TaskMetrics

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
    """
    Preprocessing step for ML further down.
    Child class of Sparker
    """
    def __init__(self, spark_df):
        self.df = spark_df
    
    def sampling(self, sample_size=0.1):
        """
        Returns a sample dataset for quick propotype 
        """
        return self.df.sample(fraction=sample_size)
    
    def split_xyz(self):
        """
        Split xyz into three columns on the dataframe
        """
        self.df = self.df.withColumn("x", col("xyz").getItem(0)) \
                        .withColumn("y", col("xyz").getItem(1)) \
                        .withColumn("z", col("xyz").getItem(2))
        return self.df
                        
    def get_null_counts_per_column(self):
        """
        Get null counts for each column.
        
        Returns:
            dict: Dictionary with column names as keys and null counts as values
        """
        null_counts = self.df.select([
            spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
            for c in self.df.columns
        ]).collect()[0]
        
        return null_counts.asDict()
    
    def get_files_with_nulls(self):
        """
        Get list of files that contain at least one null value.
        Works when multiple files are read (read_all=True).
        
        Returns:
            list: List of file paths that contain null values
        """
        if not self.read_all:
            print("Warning: read_all=False. This method works best with multiple files.")
        
        # Add filename column and check for nulls
        df_with_filename = self.df.withColumn("_filename", input_file_name())
        
        # Create null condition
        conditions = [col(c).isNull() for c in self.df.columns]
        combined_condition = reduce(lambda a, b: a | b, conditions)
        
        # Get distinct files that have nulls
        files_with_nulls = df_with_filename \
            .filter(combined_condition) \
            .select("_filename") \
            .distinct() \
            .collect()
        
        # Extract filenames from rows
        file_list = [row._filename for row in files_with_nulls]
        
        return file_list
    
    def get_null_summary_by_file(self):
        """
        Get detailed summary of null counts per file.
        
        Returns:
            list: List of tuples (filename, row_count, rows_with_nulls)
        """
        if not self.read_all:
            print("Warning: read_all=False. This method works best with multiple files.")
        
        # Add filename column
        df_with_filename = self.df.withColumn("_filename", input_file_name())
        
        # Create null indicator column
        conditions = [col(c).isNull() for c in self.df.columns]
        combined_condition = reduce(lambda a, b: a | b, conditions)
        df_with_null_flag = df_with_filename.withColumn(
            "_has_null", 
            when(combined_condition, 1).otherwise(0)
        )
        
        # Group by file and aggregate
        summary = df_with_null_flag.groupBy("_filename").agg(
            spark_sum(col("_has_null")).alias("rows_with_nulls"),
            spark_sum(when(col("_has_null") == 0, 1).otherwise(0)).alias("rows_without_nulls")
        ).collect()
        
        results = []
        for row in summary:
            filename = row._filename
            rows_with_nulls = row.rows_with_nulls
            rows_without_nulls = row.rows_without_nulls
            total_rows = rows_with_nulls + rows_without_nulls
            
            results.append({
                'filename': filename,
                'total_rows': total_rows,
                'rows_with_nulls': rows_with_nulls,
                'rows_without_nulls': rows_without_nulls
            })
        
        return results
    
    def has_nulls(self):
        """
        Check if the dataset contains any null values.
        
        Returns:
            bool: True if nulls exist, False otherwise
        """
        return self.count_rows_with_nulls() > 0
    
    

    
# Main execution
if __name__ == "__main__":
    ##
    bucket_name = "ubs-datasets/FRACTAL/data" 
    path = "train/*.parquet"
    parquet_cols = ["xyz","Intensity","Classification","Red","Green","Blue","Infrared"]

    # Create Sparker instance
    sparker = Sparker()
    
    ## Create Session 
    sparker._create_on_cluster_session()
    
    taskmetrics = TaskMetrics(sparker.spark)
    
    ## 1. Read the parquet function 
    df_train = sparker.read_parquet(bucket_name,
                                    path,
                                    read_all=False)\
                                    .select(*parquet_cols)
    
    ## PreProcessing
    preprocessing = PreProcessing(df_train)
    df_train = preprocessing.split_xyz()
    
    ## 1.1 Sampling the results for quick test
    df_train = preprocessing.sampling(fraction=0.01)
    
    ## Feature Engineering
    
    # 2. Load the variables as a vector 
    feature_cols = ['Intensity', 'Red','Green','Blue','Infrared']  # your features
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features")

    # 3. Define model
    lr = LogisticRegression(
        featuresCol="scaled_features",
        labelCol="label",
        maxIter=10
    )

    # 4. Create pipeline
    pipeline = Pipeline(stages=[assembler, scaler, lr])

    # 5. Train on ALL training data (distributed automatically)
    model = pipeline.fit(df_train)

    
    
    # Close session
    sparker.close()