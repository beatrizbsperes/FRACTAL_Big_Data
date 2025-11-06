from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, when, input_file_name
from functools import reduce
import sys
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from sparkmeasure import TaskMetrics

### COPY AND PASTE HERE THE FUNCTION WHEN PUSH IT TO THE EC2 INSTANCE
## BEFORE THAT, The solution is to just simply import the classes into this file
## delete the line right below when importing it
from src.s3handler import Sparker, PreProcessing, FeatureEngineering
    
# Main execution
if __name__ == "__main__":
    
    ## Fraction of Sampling for beggining exploration
    fraction_init = 0.01
    
    ## Name of the buckets
    bucket_name = "ubs-datasets/FRACTAL/data" 
    path_train = "train/*.parquet"
    path_validation = "validation/*.parquet"
    path_test = "test/*.parquet"
    
    ## First parquet cols to be select in order to reduce computational 
    parquet_cols = ["xyz","Intensity","Classification","Red","Green","Blue","Infrared","ReturnNumber","NumberOfReturns"]

    # Create Sparker instance
    sparker = Sparker()
    
    ## Create Session 
    sparker._create_on_cluster_session()
    
    ## Create a taskmetrics for better understand of how the cluster works 
    taskmetrics = TaskMetrics(sparker.spark)
    
    ## 1. Read the parquet function 
    df_train = sparker.read_parquet(bucket_name,
                                    path_train,
                                    read_all=False) \
                                    .select(*parquet_cols) \
                                    .sampling(fraction = fraction_init)
                                    
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

    
    
    # Close session
    sparker.close()