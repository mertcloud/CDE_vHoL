#****************************************************************************
# (C) Cloudera, Inc. 2020-2022
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# #  Author(s): Paul de Fusco, Maximilian Engelhardt
#***************************************************************************/

# NB: THIS SCRIPT REQUIRES A CDE SPARK 3 CLUSTER

#---------------------------------------------------
#               CREATE SPARK SESSION
#---------------------------------------------------

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import sys
import utils
import configparser

config = configparser.ConfigParser()
config.read("/app/mount/parameters.conf")
s3BucketName=config.get("general","s3BucketName")
username=config.get("general","username")

print("Running as Username: ", username)

spark = SparkSession \
    .builder \
    .appName("CAR SALES REPORT") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")\
    .config("spark.sql.catalog.spark_catalog.type", "hive")\
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
    .getOrCreate()

#---------------------------------------------------
#               LOAD ICEBERG TABLES AS DATAFRAMES
#---------------------------------------------------

car_sales_df = spark.sql("SELECT * FROM spark_catalog.{}_CAR_DATA.CAR_SALES".format(username))
customer_data_df = spark.sql("SELECT * FROM spark_catalog.{}_CAR_DATA.CUSTOMER_DATA".format(username))

#---------------------------------------------------
#               RUNNING DATA QUALITY TESTS
#---------------------------------------------------

# Test 1: Ensure Customer ID is Present so Join Can Happen
print("RUNNING DATA QUALITY TESTS WITH QUINN LIBRARY")
utils.test_column_presence(car_sales_df, ["customer_id"])
utils.test_column_presence(customer_data_df, ["customer_id"])

# Test 2: Spot Nulls or Blanks in Customer Data Sale Price Column:
car_sales_df = utils.test_null_presence_in_col(car_sales_df, "saleprice")

# Test 3:
customer_data_df = utils.test_values_not_in_col(customer_data_df, ["12399", "11111", "00000"], "zip")

#---------------------------------------------------
#               JOIN CUSTOMER AND SALES DATA
#---------------------------------------------------

print("EXECUTING ICEBERG CREATE OR REPLACE TABLE STATEMENT")
print("CREATE OR REPLACE TABLE spark_catalog.{0}_CAR_DATA.SALES_REPORT USING ICEBERG AS SELECT s.MODEL, s.SALEPRICE, c.SALARY, c.GENDER, c.EMAIL FROM spark_catalog.{0}_CAR_DATA.CAR_SALES s INNER JOIN spark_catalog.{0}_CAR_DATA.CUSTOMER_DATA c on s.CUSTOMER_ID = c.CUSTOMER_ID".format(username))
spark.sql("CREATE OR REPLACE TABLE spark_catalog.{0}_CAR_DATA.SALES_REPORT USING ICEBERG AS SELECT s.MODEL, s.SALEPRICE, c.SALARY, c.GENDER, c.EMAIL FROM spark_catalog.{0}_CAR_DATA.CAR_SALES s INNER JOIN spark_catalog.{0}_CAR_DATA.CUSTOMER_DATA c on s.CUSTOMER_ID = c.CUSTOMER_ID".format(username))

#---------------------------------------------------
#               ANALYTICAL QUERIES
#---------------------------------------------------

reports_df = spark.sql("SELECT * FROM {}_CAR_DATA.SALES_REPORT".format(username))

print("GROUP TOTAL SALES BY MODEL")
model_sales_df = reports_df.groupBy("Model").sum("Saleprice").na.drop().sort(F.asc("sum(Saleprice)")).withColumnRenamed("sum(Saleprice)", "sales_by_model")
model_sales_df = model_sales_df.withColumn("total_sales_by_model", model_sales_df.sales_by_model.cast(DecimalType(18, 2)))
model_sales_df.select(["Model", "total_sales_by_model"]).sort(F.asc("Model")).show()

print("GROUP TOTAL SALES BY GENDER")
gender_sales_df = reports_df.groupBy("Gender").sum("Saleprice").na.drop().sort(F.asc("sum(Saleprice)")).withColumnRenamed("sum(Saleprice)", "sales_by_gender")
gender_sales_df = gender_sales_df.withColumn("total_sales_by_gender", gender_sales_df.sales_by_gender.cast(DecimalType(18, 2)))
gender_sales_df.select(["Gender", "total_sales_by_gender"]).sort(F.asc("Gender")).show()
