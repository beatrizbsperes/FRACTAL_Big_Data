from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, when, input_file_name
from functools import reduce
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
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
from loguru import logger
import io
import time
import numpy as np
import pandas as pd
        
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
    
    def _create_on_cluster_session(self, 
                    num_executors='16', num_cores_per_executor='3',
                    executor_mem="14g", driver_mem="4g"):
        """
        Create a session to be run on the AWS EC2 cluster.
        
        Args:
            Num_cores_per_executor = Number of cores 
            Number_executors = Number of executors per core
            Executor_mem = Memory per core
            Driver_Mem = Memory of Driver
        """
        self.num_cores_per_executor = num_cores_per_executor
        self.num_executors = num_executors
        self.executor_mem = executor_mem
        self.driver_mem = driver_mem  
        
        spark = ( 
            SparkSession.builder 
                .appName("Tropa do CAGAO") 
                .config("spark.hadoop.fs.s3a.fast.upload", "true")
                .config("spark.hadoop.fs.s3a.multipart.size", "104857600")
                .config("spark.executor.memory", str(self.executor_mem ))
                .config("spark.driver.memory", str(self.driver_mem ))
                .config("spark.executor.instances", str(self.num_executors))
                .config("spark.executor.cores", str(self.num_cores_per_executor))
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") ## serialize 
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.adaptive.skewJoin.enabled", "true") \
                .config("spark.sql.adaptive.join.enabled", "true") \
                .config("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "128MB") \
                .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "2") \
                .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
                ##.config("spark.sql.shuffle.partitions", str((num_executors * num_cores_per_executor)* 2)) # 2 partitions per core ##shitzi
                .config("spark.sql.files.maxPartitionBytes", "268435456")  # 256MB
                .config("spark.driver.maxResultSize", "1g")
                .getOrCreate()
            )
        self.spark = spark
        
        print("Session Created!")
        print(f" Number of executors: {num_executors}")
        print(f" Number of cores per executor: {num_cores_per_executor}")
        print(f"- executor memory= {executor_mem}")
        print(f"- driver memory= {driver_mem}")
        
          
    def _create_local_session(self, 
                    num_executors='16', num_cores_per_executor='3',
                    executor_mem="14g", driver_mem="4g"):
        """
        Try to create a local session to run in a notebook
        """
        self.num_cores_per_executor = num_cores_per_executor
        self.num_executors = num_executors
        self.executor_mem = executor_mem
        self.driver_mem = driver_mem 
         
        access_key = self.access_key
        access_secret = self.secret_key
        
        spark = SparkSession.builder \
                .appName("Local Session my friend") \
                .master("local[4]") \
                .config("spark.jars.packages", 
                        "org.apache.hadoop:hadoop-aws:3.3.1,ch.cern.sparkmeasure:spark-measure_2.13:0.27") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.access.key", access_key) \
                .config("spark.hadoop.fs.s3a.secret.key", access_secret) \
                .config("spark.executor.instances", str(self.num_executors)) \
                .config("spark.executor.cores", str(self.num_cores_per_executor)) \
                .config("spark.executor.memory", str(self.executor_mem)) \
                .config("spark.driver.memory", str(self.driver_mem))\
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
                .config("spark.hadoop.fs.s3a.connection.timeout", "50000") \
                .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000") \
                .config("spark.hadoop.fs.s3a.multipart.purge.age", "30000000") \
                .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000") \
                .config("spark.driver.maxResultSize", "200m") \
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
        
        if len(self.file_path)<10:
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
        from pyspark.sql.functions import when, col
        
        self.df = self.df.withColumn("return_ratio", 
                                    when(col("NumberOfReturns") != 0, 
                                        col("ReturnNumber") / col("NumberOfReturns"))
                                    .otherwise(0.0)) \
                        .withColumn("is_single_return", 
                                (col("NumberOfReturns") == 1).cast("int")) \
                        .withColumn("is_last_return",
                                when(col("NumberOfReturns") != 0,
                                        (col("ReturnNumber") == col("NumberOfReturns")).cast("int"))
                                .otherwise(0))
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


def retrieve_file_names(path, percentage = None):
    """
    Match .parquet and return a list. If percentage, then return a sampled list.
    
    Args:
        lines: Path for the .txt file.
    Percentage: float 0-1
        The percentage of the total files
    """
    import re 
    from random import sample
    import random 
    random.seed(420)
    with open(path, 'r')  as file:
        lines = file.readlines()
    
    final_list =[]
    for l in lines:
        split =  l.split()
        if len(split)>=1:
            filename = split[-1]
        match = re.search(r'([A-Z0-9_-]+.parquet)', filename)
        if match:
            final_list.append(filename)
    
    if percentage is not None:
        if percentage >= 1.0:
            raise ValueError("Percentage should be a float value between 0 and 1.")
        total_num = len(final_list)
        perc = int(percentage*total_num)
        return sample(final_list, k=perc)
    
    else: 
        return final_list   
## ------------------------------------------------------------------------------

   
# Main execution
if __name__ == "__main__":
    
    ### argparse 
    parser = argparse.ArgumentParser(description="Key S3 Access args")
    parser.add_argument("--access-key",
                        required=False, help="ACCESS_KEY")
    parser.add_argument("--access-secret",
                        required=False, help="ACCESS_SECRET")
    parser.add_argument("--num-executors", 
                        type=str, required=False, help="Number of Spark Executors")
    parser.add_argument("--num-cores-per-executor", 
                        type=str, required=False, help="Cores per executor")
    parser.add_argument("--executor-mem", 
                        type=str, required=False, help="Executor memory (e.g., 4g)")
    parser.add_argument("--driver-mem", 
                        type=str, required=False, help="Driver memory (e.g., 4g)")
    parser.add_argument("--sampling", 
                        type=float, required=False, help="Percentage of the Dataset to be sampled")
    args = parser.parse_args()

    ##print(args)
    access_key = args.access_key
    access_secret = args.access_secret
    num_executors=args.num_executors
    num_cores_per_executor=args.num_cores_per_executor
    executor_mem=args.executor_mem
    driver_mem=args.driver_mem
    percentage = args.sampling
    
    ## create the name of the file 
    metrics_file = f"{percentage}perc-{num_executors}ex-{num_cores_per_executor}core-metrics"
    
    # Get the directory where this script is located and create a path on it
    script_dir = os.path.dirname(os.path.abspath(__file__))
    save_dir = os.path.join(script_dir, "metrics")
    os.makedirs(save_dir, exist_ok=True)
    
    ## Create Logger with loguru
    logger.add(f"{save_dir}/{metrics_file}.log")
    logger.info(f"All the files are being saved at: {save_dir}")
        
    ## add argparse info to logger
    logger.info(f"Num Executors:{num_executors}")
    logger.info(f"Num Cores: {num_cores_per_executor}")
    logger.info(f"Executor memory: {executor_mem}")
    logger.info(f"Driver memory: {driver_mem}")
    logger.info(f"Percentage: {percentage}")
    
    # path of the .txt file
    ## these files are the train/ test/ val/ bucket listed in a .txt
    mother= "/home/efs/erasmus/emanuel/files"
        
    ## Name of the buckets
    ## Look for the .txt file containing the name of the files
    ## percentage is the percentage of files to be retrieved from the list
    ## which is an approximation to the actual percentage sampling
    list_train = [f"train/{file}" for file in retrieve_file_names(f"{mother}/train_files.txt",percentage=percentage)]
    list_test = [f"test/{file}" for file in retrieve_file_names(f"{mother}/test_files.txt", percentage=percentage)]
    list_val = [f"val/{file}" for file in retrieve_file_names(f"{mother}/val_files.txt", percentage=percentage)]
    
    logger.info(f"Number of Train files: {len(list_train)} | Test {len(list_test)} | Val {len(list_val)}")
    
    # Bucket where the data is located.
    bucket_name = "ubs-datasets/FRACTAL/data"
    
    ## Name of the cols to be selected from each dataset in order to reduce computational cost
    parquet_cols = ["xyz","Intensity","Classification","Red","Green","Blue","Infrared","ReturnNumber","NumberOfReturns"]

    # Create Sparker instance from the Sparker class
    sparker = Sparker(
        access_key=args.access_key,
        secret_key=args.access_secret
    )
    
    # create cluster session
    sparker._create_on_cluster_session(
                num_executors=args.num_executors,
                num_cores_per_executor=args.num_cores_per_executor,
                executor_mem=args.executor_mem,
                driver_mem=args.driver_mem
    )

    ## Create a taskmetrics for better understand of how the cluster works 
    stagemetrics = StageMetrics(sparker.spark)
    
    ## START TASK METRICS 
    stagemetrics.begin()  
    
    ## Total time
    ## Time to process each task of this main
    start_time = time.time()
    
    ## 1. Read the parquet function 
    logger.info(f"Opening df train")
    df_train = sparker.read_parquet(bucket_name,
                                    list_train,
                                    read_all=False) \
                                    .select(*parquet_cols) 
    
    df_val = sparker.read_parquet(bucket_name,
                                    list_val,
                                    read_all=False) \
                                    .select(*parquet_cols)
         
    logger.info(f"Opening df test")
    df_test = sparker.read_parquet(bucket_name,
                                    list_test,
                                    read_all=False) \
                                    .select(*parquet_cols) 
    
                        
    ## PreProcessing
    logger.info(f"Preprocessing Train")
    preprocessing = PreProcessing(df_train)
    df_train = preprocessing.split_xyz()
    
    logger.info(f"Preprocessing Test")
    preprocessing_test = PreProcessing(df_test)
    df_test = preprocessing_test.split_xyz()
    
    logger.info(f"Preprocessing Val")
    preprocessing_val = PreProcessing(df_val)
    df_val = preprocessing_val.split_xyz()
    
    ## Feature Engineering
    logger.info(f"Feature Engineering | TRAIN")
    feature_eng_time = time.time()
    
    engfeature = FeatureEngineering(df_train)
    df_train = engfeature.apply_all()
    
    logger.info(f"Feature Engineering | TEST")
    engfeature_test = FeatureEngineering(df_test)
    df_test = engfeature_test.apply_all()
    
    logger.info(f"Feature Engineering | VAL")
    engfeature_test = FeatureEngineering(df_val)
    df_val = engfeature_test.apply_all()
    
    logger.info(f"TIME: Feature Engineering: {np.abs(time.time()- feature_eng_time):.4f}")
    
    ## TOTAL ROWS
    ## count the number of rows to calculate properly the percentage of data being evaluate
    ## this may be supress cause it is take time to process.
    train_rows = test_rows = None
    count_rows = time.time()
    train_rows = df_train.count()
    test_rows = df_test.count()
    logger.info(f"ROWS: TRAIN: {train_rows}")
    logger.info(f"ROWS: TEST: {test_rows}")
    total_time_to_count_rows = np.abs(time.time()-count_rows)
    logger.info(f"TIME: Count Rows: {total_time_to_count_rows:.5f}")
    
    # 2. Prepare the variables for the model  
    logger.info(f"Feature Cols | TRAIN ")
    
    ## drop classification column
    feature_cols = df_train.drop("Classification").columns   
    
    ## create a vector assembler with the feature variables
    assembler = VectorAssembler(inputCols=feature_cols,
                                outputCol="features"
                                )
    
    ## process standard scaler
    scaler = StandardScaler(inputCol="features",
                            outputCol="scaled_features"
                            )
    

    # 3. Define model
    rf = RandomForestClassifier(featuresCol="scaled_features", 
                                labelCol="Classification",
                                bootstrap=True, 
                                numTrees=50,
                                maxDepth=7)

    # 4. Create pipeline
    logger.info(f"Pipeline")
    pipeline = Pipeline(stages=[assembler, scaler, rf])
    pipe_time = time.time()
    
    # 5. Train on ALL training data (distributed automatically)
    model = pipeline.fit(df_train)
    logger.info(f"TIME: Pipeline Fit: {np.abs(time.time()-pipe_time):.5f}")
    
    # 6. Validate 
    val_time = time.time()
    predictions_val = model.transform(df_val)
    logger.info(f"TIME: Validation: {np.abs(time.time()-val_time):.5f}")
    
    # 6. Evaluate the model 
    pred_time = time.time()
    predictions = model.transform(df_test)
    logger.info(f"TIME: Test: {np.abs(time.time()-pred_time):.5f}")
    
    # 7. Test the Model
    evaluator = MulticlassClassificationEvaluator(
                        labelCol = 'Classification',
                        predictionCol = 'prediction',
                        metricName = 'accuracy'
                    )

    ## Evaluate the model on the validation and the test
    accuracy = evaluator.evaluate(predictions)
    accuracy_val = evaluator.evaluate(predictions_val)
    logger.info(f" Val Accuracy: {accuracy_val}")
    logger.info(f"Test Accuracy: {accuracy :.3f}")
    
    ## END MEASURING
    stagemetrics.end()
    final_time = time.time()
    logger.info(f"TIME: Final Time: {np.abs(final_time - start_time):.5f}")
    logger.info(f"TIME: Final Time supressed count rows: {np.abs(final_time - start_time - total_time_to_count_rows):.5f}")
    
    ## stagemetrics save
    logger.info(f"Save the stagemetrics as dictionary")
    try:
        # convert JavaMap to dict
        metrics_java = stagemetrics.aggregate_stagemetrics()
        metrics_dict = dict(metrics_java)
        
        # create the path to save it
        file_csv_name = f"{metrics_file}-stagemetrics.csv"
        csv_save_path = os.path.join(save_dir, file_csv_name)
        
        logger.info(f"Saving stagemetrics at: {csv_save_path}")
        
        ## convert to df to save the csv
        pandas_df = pd.DataFrame([metrics_dict])
        pandas_df.to_csv(csv_save_path, index=False)
        
        logger.info(f"saved stagemetrics to {csv_save_path}")
        logger.info(f"Captured {len(metrics_dict)} metrics: {list(metrics_dict.keys())[:5]}...")
        
    except Exception as e:
        logger.error(f"Failed to save stagemetrics: {str(e)}")
        logger.exception(e)
        

    ## Save the print metrics from sparkmeasure
    ## This is a test to see if the same metrics are being printed and saved
    ## and therefore a try to understand better sparkmeasure metrics
    metrics_file = f"{metrics_file}.txt"
    logger.info(f" The taskmetric is being saved at: {os.path.join(save_dir, metrics_file)}")
    with open(os.path.join(save_dir, metrics_file), 'w') as f:
        # Acuracy of the model
        f.write("=== Model Accuracy on Test Set ===\n")
        f.write("\n")
        f.write(str(round(accuracy, 3)))
        f.write("\n")

        ## TOTAL ROWS
        if (train_rows) & (test_rows):
            f.write("TOTAL ROWS")
            f.write(f"TRAIN: {train_rows}")
            f.write(f"TEST: {test_rows}")
            f.write("\n")
        
        # Cluster information
        f.write("\n")
        f.write("=== Cluster Information ===\n\n")
        f.write(f"Number of Executors: {args.num_executors}\n")
        f.write(f"Cores per Executor: {args.num_cores_per_executor}\n")
        f.write(f"Executor Memory: {args.executor_mem}\n")
        f.write(f"Driver Memory: {args.driver_mem}\n")

        ## Manipulate the stdout in order to dump into this txt file
        old_stdout = sys.stdout
        sys.stdout = mystdout = io.StringIO()
        stagemetrics.print_report()
        stagemetrics.print_memory_report()
        sys.stdout = old_stdout
        metrics_output = mystdout.getvalue()

        # Memory report and report
        f.write("\n")
        f.write("=== Report and Memory Report ===\n")
        f.write(metrics_output)
        f.write("\n")

    print("\n=====================================================\n")
    
    logger.info(f"Concluded!")
        # Close session
    sparker.close() 