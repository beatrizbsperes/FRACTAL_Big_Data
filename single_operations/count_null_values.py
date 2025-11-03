import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType

def get_string_schema():
    """Define schema to read all fields as strings initially."""
    return StructType([
        StructField("x", StringType(), nullable=True),
        StructField("y", StringType(), nullable=True),
        StructField("z", StringType(), nullable=True),
        StructField("intensity", StringType(), nullable=True),
        StructField("returnnumber", StringType(), nullable=True),
        StructField("numberofreturns", StringType(), nullable=True),
        StructField("scandirectionflag", StringType(), nullable=True),
        StructField("edgeofflightline", StringType(), nullable=True),
        StructField("classification", StringType(), nullable=True),
        StructField("synthetic", StringType(), nullable=True),
        StructField("keypoint", StringType(), nullable=True),
        StructField("withheld", StringType(), nullable=True),
        StructField("overlap", StringType(), nullable=True),
        StructField("scananglerank", StringType(), nullable=True),
        StructField("userdata", StringType(), nullable=True),
        StructField("pointsourceid", StringType(), nullable=True),
        StructField("gpstime", StringType(), nullable=True),
        StructField("scanchannel", StringType(), nullable=True),
        StructField("red", StringType(), nullable=True),
        StructField("green", StringType(), nullable=True),
        StructField("blue", StringType(), nullable=True),
        StructField("infrared", StringType(), nullable=True)
    ])

def cast_to_proper_types(df):
    """Cast string columns to proper data types."""
    return df.select(
        col("x").cast("double").alias("x"),
        col("y").cast("double").alias("y"),
        col("z").cast("double").alias("z"),
        col("intensity").cast("double").alias("intensity"),
        col("returnnumber").cast("double").alias("returnnumber"),
        col("numberofreturns").cast("double").alias("numberofreturns"),
        col("scandirectionflag").cast("double").alias("scandirectionflag"),
        col("edgeofflightline").cast("double").alias("edgeofflightline"),
        col("classification").cast("double").alias("classification"),
        col("synthetic").cast("double").alias("synthetic"),
        col("keypoint").cast("double").alias("keypoint"),
        col("withheld").cast("double").alias("withheld"),
        col("overlap").cast("double").alias("overlap"),
        col("scananglerank").cast("double").alias("scananglerank"),
        col("userdata").cast("double").alias("userdata"),
        col("pointsourceid").cast("double").alias("pointsourceid"),
        col("gpstime").cast("double").alias("gpstime"),
        col("scanchannel").cast("double").alias("scanchannel"),
        col("red").cast("double").alias("red"),
        col("green").cast("double").alias("green"),
        col("blue").cast("double").alias("blue"),
        col("infrared").cast("double").alias("infrared")
    )

def check_train_files_for_nulls(bucket_name, prefix=""):
    """
    Check all TRAIN files in S3 bucket for null values.
    
    Args:
        bucket_name: S3 bucket name
        prefix: Optional prefix/folder path in bucket
    
    Returns:
        dict: {
            'files_with_nulls': int,
            'file_names': list of str
        }
    """
    
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("S3 Train Files Null Checker") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", "<ACCESS_KEY>") \
        .config("spark.hadoop.fs.s3a.secret.key", "<SECRET_KEY>") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "50000") \
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000") \
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "30000000") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000") \
        .getOrCreate()
    
    # Get Hadoop filesystem
    sc = spark.sparkContext
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._jvm.java.net.URI.create(f"s3a://{bucket_name}"),
        sc._jsc.hadoopConfiguration()
    )
    
    # List all files in bucket
    s3_path = f"s3a://{bucket_name}/{prefix}" if prefix else f"s3a://{bucket_name}/"
    path = sc._jvm.org.apache.hadoop.fs.Path(s3_path)
    
    # Regex pattern to match TRAIN files
    train_pattern = re.compile(r'\bTRAIN\b', re.IGNORECASE)
    
    # Get string schema
    string_schema = get_string_schema()
    
    files_with_nulls = []
    null_count = 0
    
    try:
        # Recursively list all files
        file_iterator = fs.listFiles(path, True)
        
        while file_iterator.hasNext():
            file_status = file_iterator.next()
            file_path = file_status.getPath().toString()
            file_name = file_status.getPath().getName()
            
            # Check if filename contains TRAIN
            if train_pattern.search(file_name):
                print(f"Checking file: {file_name}")
                
                try:
                    # Step 1: Read with string schema
                    df = spark.read.schema(string_schema).parquet(file_path)
                    
                    # Step 2: Cast to proper types
                    df = cast_to_proper_types(df)
                    
                    # Step 3: Check for nulls in any column
                    has_nulls = False
                    for column in df.columns:
                        null_count_col = df.filter(col(column).isNull()).count()
                        if null_count_col > 0:
                            has_nulls = True
                            print(f"    Column '{column}' has {null_count_col} null values")
                    
                    if has_nulls:
                        files_with_nulls.append(file_name)
                        null_count += 1
                        print(f"  ✗ Contains null values")
                    else:
                        print(f"  ✓ No null values")
                        
                except Exception as e:
                    print(f"  Error reading file {file_name}: {str(e)}")
                    continue
    
    finally:
        spark.stop()
    
    return {
        'files_with_nulls': null_count,
        'file_names': files_with_nulls
    }


def lambda_handler(event, context):
    """
    AWS Lambda handler function.
    
    Expected event format:
    {
        "bucket_name": "your-bucket-name",
        "prefix": "optional/folder/path"
    }
    """
    
    bucket_name = event.get('bucket_name')
    prefix = event.get('prefix', '')
    
    if not bucket_name:
        return {
            'statusCode': 400,
            'body': 'bucket_name is required in event'
        }
    
    result = check_train_files_for_nulls(bucket_name, prefix)
    
    return {
        'statusCode': 200,
        'body': {
            'message': 'Null value check completed',
            'files_with_nulls_count': result['files_with_nulls'],
            'files_with_nulls': result['file_names']
        }
    }


# For local testing
if __name__ == "__main__":
    result = check_train_files_for_nulls(
        bucket_name="your-bucket-name",
        prefix="optional/prefix"
    )
    
    print("\n=== Results ===")
    print(f"Total TRAIN files with null values: {result['files_with_nulls']}")
    print(f"\nFiles containing nulls:")
    for file_name in result['file_names']:
        print(f"  - {file_name}")