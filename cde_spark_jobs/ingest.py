from pyspark.sql import SparkSession
import configparser


# create spark session
spark = SparkSession.builder.appName("INGEST").getOrCreate()

# parse job configuration
config = configparser.ConfigParser()
config.read("/app/mount/parameters.conf")
USERNAME = config.get("general", "username")
S3_BUCKET = config.get("general", "s3BucketName")
print(f"RUNNING AS USERNAME: {USERNAME}")
print(f"LOADING DATA FROM S3 BUCKET: {S3_BUCKET}\n")

for YEAR in ["2021", "2022"]:

    # append sales data
    SALES_FILEPATH = f"{S3_BUCKET}/{YEAR}/sales.csv"
    print(f"LOADING SALES DATA FROM: {SALES_FILEPATH}")
    sales_df = spark.read.csv(SALES_FILEPATH, header=True, inferSchema=True)
    sales_df.createOrReplaceTempView(f"sales_{YEAR}")
    spark.sql(f"""
        INSERT INTO car_data_{USERNAME}.sales
        SELECT customer_id, model, sales_price, to_timestamp(sales_ts), vin
        FROM sales_{YEAR}
    """)

    # upsert customers data
    CUSTOMERS_FILEPATH = f"{S3_BUCKET}/{YEAR}/customers.csv"
    print(f"LOADING CUSTOMERS DATA FROM: {CUSTOMERS_FILEPATH}")
    customers_df = spark.read.csv(CUSTOMERS_FILEPATH, header=True, inferSchema=True)
    customers_df.createOrReplaceTempView(f"customers_{YEAR}")

    spark.sql(f"""
        MERGE INTO car_data_{USERNAME}.customers t
        USING (SELECT customer_id, name, gender, occupation,
               to_date(birthdate) AS birthdate, salary FROM customers_{YEAR}) s
        ON t.customer_id = s.customer_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    print(f"FINISHED lOADING DATA FOR YEAR: {YEAR}\n")
