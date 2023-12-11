from pyspark.sql import SparkSession
import configparser

from great_expectations.dataset.sparkdf_dataset import SparkDFDataset

# parse job configuration
config = configparser.ConfigParser()
config.read("/app/mount/parameters.conf")
s3BucketName = config.get("general", "s3BucketName")
USERNAME = config.get("general", "username")
print(f"RUNNING AS USERNAME: {USERNAME}")

# create spark session
spark = SparkSession.builder.appName("VALIDATE").getOrCreate()

# validate the data quality of the sales data with great-expectations
sales_gdf = SparkDFDataset(
    spark.sql(f"SELECT * FROM car_data_{USERNAME}.sales"))
sales_gdf_validation = sales_gdf.expect_compound_columns_to_be_unique(
    ["customer_id", "VIN"])
print(f"VALIDATION RESULTS FOR SALES DATA:\n{sales_gdf_validation}\n")
assert sales_gdf_validation.success, \
    "VALIDATION FOR SALES TABLE UNSUCCESSFUL: FOUND DUPLICATES IN [customer_id, VIN]."

# validate the data quality of the customers data with great-expectations
customers_gdf = SparkDFDataset(
    spark.sql(f"SELECT * FROM car_data_{USERNAME}.customers"))
customers_gdf_validation = customers_gdf.expect_compound_columns_to_be_unique(
    ["customer_id"])
print(f"VALIDATION RESULTS FOR CUSTOMERS DATA:\n{customers_gdf_validation}\n")
assert sales_gdf_validation.success, \
    "VALIDATION FOR CUSTOMERS TABLE UNSUCCESSFUL: FOUND DUPLICATES IN customer_id."

print("FINISHED DATA QUALITY JOB: DID NOT FIND ANY QUALITY ISSUES.")
