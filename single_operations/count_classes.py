from pyspark.sql import SparkSession

spark = SparkSession.builder \
            .appName("Count_Classes") \
            .getOrCreate()
            
## Get all files that contains TRAIN and is parquet
train_data = spark.read.parquet("sa3://ubs-datasets/FRACTAL/data/TRAIN-0934_6301-008094186.parquet")

lc = train_data.groupby('classification').count()
lc.show()