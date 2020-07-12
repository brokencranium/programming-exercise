from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType
from pyspark.sql import DataFrame, SparkSession

INPUT_PATH = './resources/Order.json'
OUTPUT_CSV_PATH = './output/files/'
OUTPUT_DELTA_PATH = './output/delta/'

spark = (SparkSession
         .builder
         .appName("programming")
         .master("local[*]")
         .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .getOrCreate()
         )


def read_json(file_path: str, schema: StructType) -> DataFrame:
    """
    The goal of this method is to parse the input json data using the schema the needs to be build as part of a
    different method
    https://spark.apache.org/docs/2.4.6/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader.json
    :param file_path: Order.json will be provided
    :param schema: schema that needs to be passed to this method
    :return: Dataframe containing records from Order.json
    """
    return None


def get_struct_type() -> StructType:
    """
    Build a schema based on the the file Order.json
    :return: Structype of equivalent JSON schema
    """
    discount_type = StructType([StructField("amount", IntegerType(), True),
                                StructField("description", StringType(), True)
                                ])

    child_item_type = StructType([StructField("lineItemNumber", StringType(), True),
                                  StructField("itemLabel", StringType(), True),
                                  StructField("quantity", DoubleType(), True),
                                  StructField("price", IntegerType(), True),
                                  StructField("discounts", "TODO", True),
                                  ])

    item_type = StructType([StructField("lineItemNumber", StringType(), True),
                            StructField("itemLabel", StringType(), True),
                            StructField("quantity", DoubleType(), True),
                            StructField("price", IntegerType(), True),
                            StructField("discounts", "TODO", True),
                            StructField("childItems", "TODO", True),
                            ])

    order_paid_type = StructType([StructField("orderToken", StringType(), True),
                                  StructField("preparation", StringType(), True),
                                  StructField("items", "TODO", True),
                                  ])

    message_type = StructType([StructField("orderPaid", "TODO", True)])

    data_type = StructType([StructField("message", "TODO", True)])

    body_type = StructType([StructField("id", StringType(), True),
                            StructField("subject", StringType(), True),
                            StructField("data", "TODO", True),
                            StructField("eventTime", StringType(), True),
                            ])
    return None


def get_rows_from_array(df: DataFrame) -> DataFrame:
    """
    Input data frame contains columns of type array. Identify those columns and convert them to rows.
    https://spark.apache.org/docs/2.4.6/api/python/pyspark.sql.html
    :param df: Contains column with data type of type array.
    :return: The dataframe should not contain any columns of type array
    """
    return None


def get_unwrapped_nested_structure(df: DataFrame) -> DataFrame:
    """
    Convert columns that contain multiple attributes to columns of their own
    https://spark.apache.org/docs/2.4.6/api/python/pyspark.sql.html#pyspark.sql.DataFrame.selectExpr
    :param df: Contains columns that have multiple attributes
    :return: Dataframe should not contain any nested structures
    """
    return None


def write_df_as_csv(df: DataFrame):
    """
    Write the data frame to a local local destination of your choice with headers
    https://spark.apache.org/docs/2.4.6/api/python/pyspark.sql.html#pyspark.sql.DataFrameWriter.csv
    :param df: Contains flattened order data
    """
    pass


def create_delta_table(spark: SparkSession):
    spark.sql('CREATE DATABASE IF NOT EXISTS SBUX')

    spark.sql('''
    CREATE TABLE IF NOT EXISTS SBUX.ORDERS(
        OrderToken String,
        Preparation  String,
        ItemLineNumber String,
        ItemLabel String,
        ItemQuantity Double,
        ItemPrice Integer,
        ItemDiscountAmount Integer,
        ItemDiscountDescription String,
        ChildItemLineNumber String, 
        ChildItemLabel String,
        ChildItemQuantity Double,
        ChildItemPrice Integer,
        ChildItemDiscountAmount Integer,
        ChildItemDiscountDescription String
    ) USING DELTA
    LOCATION "{}"
    '''.format(OUTPUT_DELTA_PATH))


def write_df_as_delta(df: DataFrame):
    """
    Write the dataframe output to the table created, overwrite mode can be used
    https://spark.apache.org/docs/2.4.6/api/python/pyspark.sql.html#pyspark.sql.DataFrameWriter
    :param df: flattened data
    :return: Data from the orders table
    """

    pass


def read_data_delta(spark: SparkSession) -> DataFrame:
    """
    Read data from the table created
    https://spark.apache.org/docs/2.4.6/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader.format
    :param spark:
    :return:
    """

    return None


if __name__ == '__main__':
    input_schema = get_struct_type()

    input_df = read_json(INPUT_PATH, input_schema)
    # input_df.show(truncate=False)

    arrays_to_rows_df = get_rows_from_array(input_df)
    # arrays_to_rows_df.show(truncate=False)

    unwrap_struct_df = get_unwrapped_nested_structure(arrays_to_rows_df)
    # unwrap_struct_df.show(truncate=False)

    write_df_as_csv(unwrap_struct_df)

    create_delta_table(spark)
    write_df_as_delta(unwrap_struct_df)

    result_df = read_data_delta(spark)
    result_df.show(truncate=False)
