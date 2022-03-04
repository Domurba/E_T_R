from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    IntegerType,
    FloatType,
    TimestampType,
)
from pyspark.sql.functions import monotonically_increasing_id, broadcast, lit
import sys
import psycopg2
from datetime import datetime

# The path can be either a single CSV file or a directory of CSV files
file = sys.argv[1]
postgres_db = sys.argv[2]
postgres_user = sys.argv[3]
postgres_pwd = sys.argv[4]


uri = f"postgresql://{postgres_user}:{postgres_pwd}@postgres/postgres"


# Connecting to postgres, inserting current date and time and returning the batch id
with psycopg2.connect(uri) as conn:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO datetime_of_insert (insert_date) VALUES (%s) RETURNING batch_id",
            (datetime.now(),),
        )
        batch_id = cur.fetchone()[0]
conn.close()

spark = SparkSession.builder.getOrCreate()


schema1 = StructType(
    [
        StructField("event_id", IntegerType()),
        StructField("event_name", StringType()),
    ]
)
# Date time object is converted to UTC
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
# Reading the small event type table. It is later bradcasted for easier joins
df_event = (
    spark.read.format("jdbc")
    .option("url", postgres_db)
    .option("dbtable", "d_event_type")
    .option("schema", schema1)
    .option("user", postgres_user)
    .option("password", postgres_pwd)
    .load()
)

# Reading the initial csv file with the provided shcema
df = spark.read.csv(file, header=False, schema=schema).drop("Month")

# Joining the initial csv with the event type table, selecting only relavant columns
df2 = df.join(
    broadcast(df_event), df.Event_Type == df_event.event_name, "inner"
).select(
    df.Customer_id,
    df.start_D,
    df.Rate_Plan_ID,
    df.Flag1,
    df.Flag2,
    df.Duration,
    df.Charge,
    df_event.event_id,
)
# This view will be used in the below sql query in order to remove rows containin duplicate (customer_id, start_d and event_id)
df2.createOrReplaceTempView("FOR_UNIQUE_PK")

df3 = spark.sql(
    """
    SELECT a.*
    FROM FOR_UNIQUE_PK a
    JOIN
    (SELECT Customer_id, start_D, event_id FROM 
    FOR_UNIQUE_PK
    GROUP BY Customer_id, start_D, event_id
    HAVING COUNT(*) = 1) b
    on a.Customer_id = b.Customer_id AND a.start_D = b.start_D AND A.event_id = B.event_id
    """
)

# view and query used to insert values into the SCD2 table. Additional surrogate key is added in order to identify it with the main fact table.
df3.createOrReplaceTempView("FOR_SCD2")
df4 = (
    spark.sql(
        """
    with cte as (
    SELECT Customer_id, Rate_Plan_ID, min(start_D) as start_D
    from FOR_SCD2
    group by Customer_id, Rate_Plan_ID
    ),
    tbl as (
    select *,
    CASE WHEN
    last_value(Rate_Plan_ID) 
    over (partition by Customer_id order by start_D 
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) = Rate_Plan_ID
    THEN true
    ELSE false
    END is_valid
    from cte
    )
    SELECT *,
    CASE WHEN
    is_valid = false
    THEN lead(start_D) over (partition by Customer_id order by start_D)
    ELSE CAST('9999-12-31' AS DATE)
    END end_d
    from tbl
    """
    )
    .withColumn("surrogate_uid", monotonically_increasing_id())
    .select(
        "surrogate_uid",
        "customer_id",
        "Rate_plan_id",
        "start_D",
        "End_d",
        "is_valid",
    )
)

# Loading the SCD2 table into postgres
try:
    (
        df4.write.format("jdbc")
        .option("url", postgres_db)
        .option("dbtable", "d_scd2_rate_plan")
        .option("user", postgres_user)
        .option("password", postgres_pwd)
        .mode("append")
        .save()
    )
except:
    print("duplicate data in d_scd2_rate_plan table, passing")

# the data used for f_usage table. Join is used in order to get the surrogate_uid, relating to a user and their plan at the time of the event. batch id is additionaly added.
df4.createOrReplaceTempView("D_surrogates")
df5 = spark.sql(
    """
    SELECT 
    DS.surrogate_uid,
    FT.Customer_id,
    FT.start_D,
    FT.event_id,
    FT.Flag1,
    FT.Flag2,
    FT.Duration,
    FT.Charge
    from FOR_SCD2 FT
    JOIN D_surrogates DS 
    ON FT.Customer_id = DS.customer_id AND
    FT.Rate_Plan_ID = DS.Rate_Plan_ID AND
    FT.start_D >= DS.start_D AND
    FT.start_D < DS.end_D

"""
).withColumn("batch_id", lit(batch_id))

# Loading the main fact table into postgres
try:
    (
        df5.write.format("jdbc")
        .option("url", postgres_db)
        .option("dbtable", "f_usage")
        .option("user", postgres_user)
        .option("password", postgres_pwd)
        .mode("append")
        .save()
    )
except:
    print("duplicate data in f_usage table, passing")
