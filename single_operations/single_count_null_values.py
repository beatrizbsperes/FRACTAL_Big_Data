from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType

def get_string_schema():
    """Define schema to read all fields as strings initially."""
    return StructType([
        StructField("x", StringType(), nullable=True),
        StructField("y", StringType(), nullable=True),
        StructField("z", StringType(), nullable=True),
        StructField("intensity", (), nullable=True),
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

def check_single_file_for_nulls(file_path):
    """
    Check a single parquet file for null values.
    
    Args:
        file_path: Full S3 path to the parquet file (e.g., s3a://bucket-name/path/to/file.parquet)
    
    Returns:
        dict: Information about null values in the file
    """
    
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("Single File Null Checker") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", "your_key") \
        .config("spark.hadoop.fs.s3a.secret.key", "your_secret") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "50000") \
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000") \
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "30000000") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000") \
        .getOrCreate()
        
    ##spark.setLogLevel("WARN")
    try:
        print(f"Reading file: {file_path}")
        
        # Get string schema
        #string_schema = get_string_schema()
        
        # Step 1: Read with string schema
        df = spark.read.option("header", "true").option("inferSchema", "true").parquet(file_path)
        
        print(f"Total rows: {df.count()}")
        
        # Step 2: Cast to proper types
        df = cast_to_proper_types(df)
        
        print("\nSchema after casting:")
        df.printSchema()
        
        # Step 3: Check for nulls in each column
        print("\n" + "="*60)
        print("NULL VALUE CHECK")
        print("="*60)
        
        has_nulls = False
        null_details = {}
        
        for column in df.columns:
            null_count = df.filter(col(column).isNull()).count()
            null_details[column] = null_count
            
            if null_count > 0:
                has_nulls = True
                print(f"✗ {column:20s}: {null_count:,} null values")
            else:
                print(f"✓ {column:20s}: No nulls")
        
        print("="*60)
        
        if has_nulls:
            print("\n⚠️  FILE CONTAINS NULL VALUES")
        else:
            print("\n✓ FILE IS CLEAN - NO NULL VALUES")
        
        return {
            'has_nulls': has_nulls,
            'null_details': null_details,
            'total_rows': df.count()
        }
        
    except Exception as e:
        print(f"Error reading file: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 3:
        print("Usage: spark-submit --master local[4] --packages org.apache.hadoop:hadoop-aws:3.3.1 test_single_file.py <bucket_name> <file_name>")
        print("\nExample:")
        print("  spark-submit --master local[4] --packages org.apache.hadoop:hadoop-aws:3.3.1 test_single_file.py my-bucket TRAIN-1200_6136-008972557.parquet")
        print("\nOr with full path:")
        print("  spark-submit --master local[4] --packages org.apache.hadoop:hadoop-aws:3.3.1 test_single_file.py my-bucket path/to/TRAIN-1200_6136-008972557.parquet")
        sys.exit(1)
    
    bucket_name = sys.argv[1]
    file_path = sys.argv[2]
    
    # Construct full S3 path
    if file_path.startswith("s3a://"):
        full_path = file_path
    else:
        full_path = f"s3a://{bucket_name}/{file_path}"
    
    print(f"Testing file: {full_path}\n")
    
    result = check_single_file_for_nulls(full_path)