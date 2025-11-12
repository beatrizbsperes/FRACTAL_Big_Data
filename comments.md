
## Executor Memory

## Driver Memory

## Executor Instances

## Executor Cores 

## Shuffle Partitions

Shuffle: A process that moves data across the network to redistribute it for downstream operations, such as joins or aggregations. It’s an expensive operation due to network I/O.

For this we have use Adaptative Coaslecence 
[Check](https://krishna-yogik.medium.com/spark-aqe-a-detailed-guide-with-examples-d8b52a0a2f20)


This parameter specifies the number of partitions used during shuffle operations for DataFrame where the default value is 200. 
However, since the executor cores and number of executors are being adjusted by hand, we decided to use from 2-4 partitions per core, which implies on the following math:

(Number of Cores * Number of Executors) = Total Number of Workers in Parallel
And then 
(Number of Cores * Number of Executors)*Number_of_Partions_per_core(2) = Shuffle Partitions

Where the end of the calculation is the 

## Tuning the Paralellism

Many of the tunning comes from Spark Documentation [Spark Apache](https://spark.apache.org/docs/latest/sql-performance-tuning.html#tuning-partitions)

## Understanding about partition

Each partition is a chunk of data that can be processed independently on a worker node in the cluster.

When Spark runs a job, it divides your dataset into multiple partitions.
Then, each partition is processed in parallel by different executors or cores.

The number and size of partitions affect performance directly, whereas too few partitions can not optimize the pararelism (under-utilized cluster) and too many 
small partitions can cause overhead in scheduling tasks and managing metadata.

These partitions tuning can be accessed on spark at the property `spark.sql.files.maxPartitionBytes` where by default is `128MB`

On our code, we optimized the partition by 256MB. : 
`.config("spark.sql.files.maxPartitionBytes", "268435456")` 

## Adaptative Query Execution

Adaptive Query Execution (AQE) is an optimization technique in Spark SQL that makes use of the runtime statistics to choose the most efficient query execution plan. This is optimized by running: 

`spark.sql.adaptive.enabled`


## Serializer 

We tried a new serializer, that was given by: 
`spark.sql.adaptive.enabled` 

## Multipar Upload

`spark.hadoop.fs.s3a.fast.upload=true` -> Enables faster buffering to disk.

`.config("spark.hadoop.fs.s3a.multipart.size", "104857600"`
This linerefers to the size of each part when Spark upload files to S3, which is equal to 100MB.
This control how big each part is uploaded to the S3 connector.
