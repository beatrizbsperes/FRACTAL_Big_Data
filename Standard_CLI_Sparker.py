from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, when, input_file_name
from functools import reduce
import sys
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from sparkmeasure import TaskMetrics
import os
from datetime import datetime
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
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
from pyspark.sql.functions import coalesce, lit
from sparkmeasure import StageMetrics
import argparse
import sys
from loguru import logger
## -------------------------------------------------------------------------------
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
        self.num_cores_per_executor = "4"
        self.num_executors = "7"
        self.executor_mem = "28g"
        self.driver_mem = "8g"
    
        
    def _create_on_cluster_session(self, num_executors='16', num_cores_per_executor='3',
                                            executor_mem="14g", driver_mem="4g"):
        """
        Create a session to be run on the AWS EC2 cluster.
        
        Args:
            num_executors (int): Number of executors ot create the task. 
            num_cores_per_executor (int): 
                Number of cores per executor. It can be also seen as the number of threads. 
            executor_mem: str (e.g: 4g) = Memory of the executor node.
            driver_mem: str (e.g: 4g) = Memory of the Driver node.
        """
        if num_executors==None:
            num_executors= self.num_executors
        if num_cores_per_executor==None:
            num_cores_per_executor= self.num_cores_per_executor
        if executor_mem==None:
            executor_mem= self.executor_mem
        if driver_mem==None:
            driver_mem = self.driver_mem
            
        spark = ( 
            SparkSession.builder 
                .appName("Read FRACTAL files") 
                .config("spark.hadoop.fs.s3a.fast.upload", "true")
                .config("spark.hadoop.fs.s3a.multipart.size", "104857600")
                .config("spark.executor.memory",executor_mem )
                .config("spark.driver.memory", driver_mem )
                .config("spark.executor.instances", str(num_executors))
                .config("spark.executor.cores", str(num_cores_per_executor))
                .getOrCreate()
            )
        self.spark = spark
        
        print("Session Created!")
        print(f" Number of executors: {num_executors}")
        print(f" Number of cores per executor: {num_cores_per_executor}")
        print(f"- executor memory= {executor_mem}")
        print(f"- driver memory= {driver_mem}")
        
          
    def _create_local_session(self, access_key, access_secret):
        """
        Try to create a local session to run in a notebook
        """
        spark = SparkSession.builder \
                .appName("Local Session my friend") \
                .master("local[4]") \
                .config("spark.jars.packages", 
                        "org.apache.hadoop:hadoop-aws:3.3.1,ch.cern.sparkmeasure:spark-measure_2.13:0.27") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.access.key", access_key) \
                .config("spark.hadoop.fs.s3a.secret.key", access_secret) \
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
            self.file_path = [f"s3a://{bucket_name}/{path}"]  # Make it a list for uniform handling

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
        return self.df
                        
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
        
        # local statistics with null handling
        self.df = self.df.withColumn("local_density", count("*").over(window)) \
                        .withColumn("local_z_std", 
                                coalesce(stddev("z").over(window), lit(0.0))) \
                        .withColumn("local_z_range", 
                                spark_max("z").over(window) - spark_min("z").over(window))
        
        # roughness (normalized std) with null handling
        self.df = self.df.withColumn("roughness", 
                                    coalesce(
                                        col("local_z_std") / (col("local_z_range") + 0.01),
                                        lit(0.0)
                                    ))
        
        self.df = self.df.drop("lx", "ly")
        return self.df
    
    def number_of_return(self):
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
    
    def water_detection(self):
        """
        NDWI for water detection
        """
        self.df = self.df.withColumn("ndwi", (col("Green") - col("Infrared")) / (col("Green") + col("Infrared") + 0.001))
        return self.df
    
    def drop_xyz(self):
        self.df = self.df.drop('xyz')
        return self.df
    
    def apply_all(self):
        """Apply all feature engineering steps"""
        self.height_above_ground()
        self.local_stats()
        self.number_of_return()
        self.vegetation_index()
        self.water_detection()
        self.drop_xyz()
        return self.df 

## ------------------------------------------------------------------------------

   
# Main execution
if __name__ == "__main__":
    ### argparse 
    parser = argparse.ArgumentParser(description="Key S3 Access args")
    parser.add_argument("--access-key",
                        required=False, help="ACCESS_KEY")
    parser.add_argument("--access-secret",
                        required=False, help="ACCESS_SECRET")
    args = parser.parse_args()

    print(args)
    access_key = args.access_key
    access_secret = args.access_secret
    
    ## Start Logger and name of the file 
    datetime_now = datetime.today()
    metrics_file = f"{datetime_now.strftime("%d-%m-%Y_%Hh-%Mmin")}-metrics.txt"
    bucket_end ="metrics" ### "s3a://ubs-homes/erasmus/emanuel/" 
    logger.add(f"{bucket_end}/{metrics_file}.log")
    
    
    ## Fraction of Sampling for beggining exploration
    #fraction_init = 0.01 #5% of the dataset 
    
    ## Name of the buckets
    bucket_name = "ubs-datasets/FRACTAL/data" 
    path_train = ["train/TRAIN-1200_6136-008972557.parquet"]
    #path_validation = "validation/*.parquet"
    path_test = ["train/TRAIN-0436_6399-002955400.parquet"]
    #path_test = "test/*.parquet"
    
    
    ## First parquet cols to be select in order to reduce computational 
    parquet_cols = ["xyz","Intensity","Classification","Red","Green","Blue","Infrared","ReturnNumber","NumberOfReturns"]

    # Create Sparker instance
    sparker = Sparker()
    
    ## Create Session 
    sparker._create_local_session(access_key = access_key, 
                                  access_secret = access_secret)
    
    ## Create a taskmetrics for better understand of how the cluster works 
    ##taskmetrics = TaskMetrics(sparker.spark)
    stagemetrics = StageMetrics(sparker.spark)
    
    ## START TASK METRICS                     
    #taskmetrics.begin() 
    stagemetrics.begin()  
    
    ## 1. Read the parquet function 
    logger.info(f"Opening df train")
    df_train = sparker.read_parquet(bucket_name,
                                    path_train,
                                    read_all=False) \
                                    .select(*parquet_cols) \
                                    #.sample(fraction = fraction_init)
    
    # df_val = sparker.read_parquet(bucket_name,
    #                                 path_validation,
    #                                 read_all=False) \
    #                                 .select(*parquet_cols) \
    #                                 .sample(fraction = fraction_init)      
    logger.info(f"Opening df test")
    df_test = sparker.read_parquet(bucket_name,
                                    path_test,
                                    read_all=False) \
                                    .select(*parquet_cols) \
                                    #.sample(fraction = fraction_init)
    
                        
    ## PreProcessing
    logger.info(f"Preprocessing Train")
    preprocessing = PreProcessing(df_train)
    df_train = preprocessing.split_xyz()
    
    logger.info(f"Preprocessing Test")
    preprocessing_test = PreProcessing(df_test)
    df_test = preprocessing_test.split_xyz()
    
    ## Feature Engineering
    logger.info(f"Feature Engineering | TRAIN")
    engfeature = FeatureEngineering(df_train)
    df_train = engfeature.apply_all()
    
    logger.info(f"Feature Engineering | TEST")
    engfeature_test = FeatureEngineering(df_test)
    df_test = engfeature_test.apply_all()
    
    # 2. Prepare the variables for the model  
    logger.info(f"Feature Cols | TRAIN ")
    feature_cols = df_train.drop("Classification").columns   
    assembler = VectorAssembler(inputCols=feature_cols,
                                outputCol="features"
                                )
    scaler = StandardScaler(inputCol="features",
                            outputCol="scaled_features"
                            )
    

    # 3. Define model
    lr = LogisticRegression(
            featuresCol="scaled_features",
            labelCol="Classification",
            maxIter=10
        )

    # 4. Create pipeline
    pipeline = Pipeline(stages=[assembler, scaler, lr])

    # 5. Train on ALL training data (distributed automatically)
    model = pipeline.fit(df_train)

    # 6. Evaluate the model 
    predictions = model.transform(df_test)
    
    # 7. Test the Model
    evaluator = MulticlassClassificationEvaluator(
                        labelCol = 'Classification',
                        predictionCol = 'Prediction',
                        metricName = 'accuracy'
                    )

    accuracy = evaluator.evaluate(predictions)
    print(f"Test Accuracy: {accuracy :.3f}")
    
    ## spark METRICS
        ## END MEASURING
    stagemetrics.end()
    #taskmetrics.end()
    ## Print reports
    logger.info(f"{stagemetrics.print_report()}")
    logger.info(f"{stagemetrics.print_memory_report()}")
    
    
    ## Make the Daily dir to save the output of the print statement
    os.makedirs(bucket_end,exist_ok=True)
    
    ## Save the data at 
    logger.info(f" The taskmetric is being saved at: {os.path.join(bucket_end, metrics_file)}")
    # taskmetrics.save_data(os.path.join(bucket_end, metrics_file))
    ## Save the metrics to file
    with open(os.path.join(bucket_end, metrics_file), 'w') as f:
        # Get metrics as dictionary
        metrics = stagemetrics
        f.write(str(metrics))
        
    print("\n=====================================================\n")
    
    # Close session
    sparker.close()