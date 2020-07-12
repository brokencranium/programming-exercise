## Set Up
1. maven needs to be set up on the local machine
2. pyspark version 3.0.0 is required

## Goal 
The goal of the programming exercise is 
1. Read data from a json file without inferring schema.
1. Explode arrays 
1. Unwrap nested structures.
1. Write the dataframe to csv
1. Create delta table
1. Write to table 
1. Read from table


## Additional questions 

1. Spark cluster components and deployment modes
1. Caching - cache(), persist(), unpersist(), and storage levels
1. Partitioning 
    1. Initial DataFrame partitioning when reading from data source
    1. Repartitioning via coalesce() vs repartition()
    1. Controlling number of shuffle partitions
1. Performance
    1. Catalyst optimizer
    1. Identifying performance bottlenecks in Spark applications
1. Transformations, actions, and other operations
    1. Wide vs Narrow
1. Joins
    1. Broadcast Joins
    1. Cross Joins 
1. Defining and using User Defined Functions (UDFs)
1. Window functions    
1. Streaming
    1. Checkpoints
    1. Aggregation using time windows
    1. Watermarking
