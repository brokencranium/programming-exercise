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
    different method. The data that is purposeful for this excercie is from orderPaid. Return the data
    :param file_path: Order.json will be provided
    :param schema: schema that needs to be passed to this method
    :return: Dataframe containing records from Order.json
    """
    # Reference https://spark.apache.org/docs/2.4.6/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader.json
    # spark.read...
    raw_df = (spark
              .read
              .option("multiline", "true")
              .json(file_path, schema)
              .selectExpr("data.message.orderPaid.*")
              )
    return raw_df


def get_struct_type() -> StructType:
    """
    Build a schema based on the the file Order.json
    :return:
    """
    discount_type = StructType([StructField("amount", IntegerType(), True),
                                StructField("description", StringType(), True)
                                ])

    child_item_type = StructType([StructField("lineItemNumber", StringType(), True),
                                  StructField("itemLabel", StringType(), True),
                                  StructField("quantity", DoubleType(), True),
                                  StructField("price", IntegerType(), True),
                                  StructField("discounts", ArrayType(discount_type, True), True),
                                  ])

    item_type = StructType([StructField("lineItemNumber", StringType(), True),
                            StructField("itemLabel", StringType(), True),
                            StructField("quantity", DoubleType(), True),
                            StructField("price", IntegerType(), True),
                            StructField("discounts", ArrayType(discount_type, True), True),
                            StructField("childItems", ArrayType(child_item_type, True), True),
                            ])

    order_paid_type = StructType([StructField("orderToken", StringType(), True),
                                  StructField("preparation", StringType(), True),
                                  StructField("items", ArrayType(item_type, True), True),
                                  ])

    message_type = StructType([StructField("orderPaid", order_paid_type, True)])

    data_type = StructType([StructField("message", message_type, True)])

    body_type = StructType([StructField("id", StringType(), True),
                            StructField("subject", StringType(), True),
                            StructField("data", data_type, True),
                            StructField("eventTime", StringType(), True),
                            ])
    return body_type


def get_rows_from_array(df: DataFrame) -> DataFrame:
    """
    Input data frame contains columns of type array. Identify those columns and convert them to rows.

    :param df: Contains column with data type of type array.
    :return: The dataframe should not contain any columns of type array
    """
    # https://spark.apache.org/docs/2.4.6/api/python/pyspark.sql.html

    array_explode_df = (df
                        .selectExpr("*", "explode_outer(items) as item")
                        .selectExpr("*", "explode_outer(item.discounts) as itemDiscount")
                        .selectExpr("*", "explode_outer(item.childItems) as childItem")
                        .selectExpr("*", "explode_outer(childItem.discounts )as childItemDiscount")
                        )
    return array_explode_df


def get_unwrapped_nested_structure(df: DataFrame) -> DataFrame:
    """
    Convert columns that contain multiple attributes to columns of their own
    :param df: Contains columns that have multiple attributes
    :return: Dataframe should not contain any nested structures
    """
    struct_explode_df = (df
                         .selectExpr("orderToken as OrderToken",
                                     "preparation as Preparation",
                                     "item.lineItemNumber as ItemLineNumber",
                                     "item.itemLabel as ItemLabel",
                                     "item.quantity as ItemQuantity",
                                     "item.price as ItemPrice",
                                     "itemDiscount.amount as ItemDiscountAmount",
                                     "itemDiscount.description as ItemDiscountDescription",
                                     "childItem.lineItemNumber as ChildItemLineNumber",
                                     "childItem.itemLabel as ChildItemLabel",
                                     "childItem.quantity as ChildItemQuantity",
                                     "childItem.price as ChildItemPrice",
                                     "childItemDiscount.amount as ChildItemDiscountAmount",
                                     "childItemDiscount.description as ChildItemDiscountDescription"
                                     )
                         )
    return struct_explode_df


def write_df_as_csv(df: DataFrame):
    """
    Write the data frame to a local local destination of your choice with headers
    :param df: Contains flattened order data
    """
    # https://spark.apache.org/docs/2.4.6/api/python/pyspark.sql.html#pyspark.sql.DataFrameWriter.csv
    (df
     .write
     .csv(OUTPUT_CSV_PATH, mode="overwrite", header=True)
     )


def write_df_as_delta(df: DataFrame):
    """

    :param df:
    :return:
    """
    (df
     .write
     .format("delta")
     .mode("overwrite")
     .saveAsTable("sbux.orders")
     )


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


def read_data_delta(spark) -> DataFrame:
    result_df = spark.read.format('delta').table('sbux.orders')
    return result_df


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
