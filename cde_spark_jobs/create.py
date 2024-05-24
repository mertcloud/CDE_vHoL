from pyspark.sql import SparkSession


# create spark session
spark = SparkSession.builder.appName("CREATE").getOrCreate()

USERNAME = spark._sc.sparkUser()
print(f"RUNNING AS USERNAME: {USERNAME}")

# database cleanup and create
spark.sql(f"DROP DATABASE IF EXISTS car_data_{USERNAME} CASCADE")
spark.sql(f"CREATE DATABASE IF NOT EXISTS car_data_{USERNAME}")
print(f"(RE-)CREATED DATABASE: car_data_{USERNAME}")

# create sales table
spark.sql(f"""
CREATE TABLE car_data_{USERNAME}.sales(
    customer_id int,
    model string,
    sales_price double,
    sales_date date,
    vin string )
USING ICEBERG TBLPROPERTIES ('format-version' = '2')
""")
print(f"FINISHED CREATING TABLE: car_data_{USERNAME}.sales")
spark.sql(f"SHOW CREATE TABLE car_data_{USERNAME}.sales").show(100, False)

# create customers table
spark.sql(f"""
CREATE TABLE car_data_{USERNAME}.customers(
    customer_id int,
    name string,
    gender string,
    occupation string,
    birthdate date,
    salary double)
USING ICEBERG TBLPROPERTIES ('format-version' = '2')
""")
print(f"FINISHED CREATING TABLE: car_data_{USERNAME}.customers")
spark.sql(f"SHOW CREATE TABLE car_data_{USERNAME}.customers").show(100, False)

print("FINISHED CREATE TARGET SCHEMA JOB.")
