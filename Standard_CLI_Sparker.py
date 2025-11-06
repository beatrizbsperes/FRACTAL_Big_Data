from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, when, input_file_name
from functools import reduce
import sys
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from sparkmeasure import TaskMetrics
import os
from datetime import datetime

### COPY AND PASTE HERE THE FUNCTION WHEN PUSH IT TO THE EC2 INSTANCE
## BEFORE THAT, The solution is to just simply import the classes into this file
## delete the line right below when importing it
from src.s3handler import Sparker, PreProcessing, FeatureEngineering
    
# Main execution
if __name__ == "__main__":
    datetime_now = datetime.today()
    
    ## Fraction of Sampling for beggining exploration
    fraction_init = 0.01 #1% of the dataset 
    
    ## Name of the buckets
    bucket_name = "ubs-datasets/FRACTAL/data" 
    path_train = "train/*.parquet"
    path_validation = "validation/*.parquet"
    path_test = "test/*.parquet"
    
    
    metrics_file = f"metrics/{}"
    ## First parquet cols to be select in order to reduce computational 
    parquet_cols = ["xyz","Intensity","Classification","Red","Green","Blue","Infrared","ReturnNumber","NumberOfReturns"]

    # Create Sparker instance
    sparker = Sparker()
    
    ## Create Session 
    sparker._create_on_cluster_session()
    
    ## Create a taskmetrics for better understand of how the cluster works 
    taskmetrics = TaskMetrics(sparker.spark)
    
    ## START TASK METRICS                     
    taskmetrics.begin() 
      
    ## 1. Read the parquet function 
    df_train = sparker.read_parquet(bucket_name,
                                    path_train,
                                    read_all=False) \
                                    .select(*parquet_cols) \
                                    .sample(fraction = fraction_init)
    
    df_val = sparker.read_parquet(bucket_name,
                                    path_validation,
                                    read_all=False) \
                                    .select(*parquet_cols) \
                                    .sample(fraction = fraction_init)      
    
    df_test = sparker.read_parquet(bucket_name,
                                    path_test,
                                    read_all=False) \
                                    .select(*parquet_cols) \
                                    .sample(fraction = fraction_init)
    
                        
    ## PreProcessing
    preprocessing = PreProcessing(df_train)
    df_train = preprocessing.split_xyz()
    
    ## Feature Engineering
    engfeature = FeatureEngineering(df_train)
    df_train = engfeature.apply_all()
    
    # 2. Prepare the variables for the model  
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
    
    
    # 7. Test the Model
    
    
    
    
    taskmetrics.end()
    taskmetrics.print_report()
    taskmetrics.print_memory_report()
    ## Save data
    bucket_end = "s3a://ubs-homes/erasmus/emanuel/"
    output_dir = f"metrics/{datetime_now.strftime("%Y-%m-%d")}" 
    file_name = f"{datetime_now.strftime("%Hh-%Mmin")}-metrics.txt"
    
    ## Make the Daily dir to save the output of the print statement
    os.makedirs(os.path.join(bucket_end, output_dir),
                                exist_ok=True)
    ## Save the data at 
    print(f" The taskmetric is being saved at: {os.path.join(bucket_end, output_dir, file_name)}")
    taskmetrics.save_data(os.path.join(bucket_end, output_dir, file_name))
    
    print("\n=====================================================\n")
    # Close session
    sparker.close()