from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    IntegerType,
    FloatType,
    TimestampType,
)

import sys

# The path can be either a single CSV file or a directory of CSV files
file = sys.argv[1]
postgres_db = sys.argv[2]
postgres_user = sys.argv[3]
postgres_pwd = sys.argv[4]

spark = SparkSession.builder.getOrCreate()

schema = StructType(
    [
        StructField("Customer_id", IntegerType(), False),
        StructField("start_D", TimestampType(), False),
        StructField("Event_Type", StringType(), False),
        StructField("Rate_Plan_ID", IntegerType(), False),
        StructField("Flag1", IntegerType(), False),
        StructField("Flag2", IntegerType(), False),
        StructField("Duration", IntegerType(), False),
        StructField("Charge", FloatType(), False),
        StructField("Month", StringType(), False),
    ]
)


df = spark.read.csv(file, header=False, schema=schema).drop("Month")


df.createOrReplaceTempView("PK_CHECK")

df2 = spark.sql(
    """
        SELECT a.*
        FROM PK_CHECK a
        JOIN
       (SELECT Customer_id, start_D, Event_Type FROM 
        PK_CHECK
        GROUP BY Customer_id, start_D, Event_Type
        HAVING COUNT(*) > 1) b
        on a.Customer_id = b.Customer_id AND a.start_D = b.start_D AND A.Event_Type = B.Event_Type

        """
)
# Already checked for null values in the whole dataset, and found none (df.isnull().values.any()).
# would possibly need to perform this check after receiving datasets in thefuture

if len(df2.head(1)) > 0:
    df2.toPandas().to_csv("/usr/local/spark/resources/duplicates.csv")
