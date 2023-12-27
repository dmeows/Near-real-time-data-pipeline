import os
import time
import datetime
import pyspark.sql.functions as sf
import pyspark.sql.utils
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import when
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql.functions import lit
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.window import Window as W
import time_uuid
import uuid
from login_mysql import USER, PASSWORD, HOST, PORT, DB_NAME, URL, DRIVER


def create_spark_session():
    spark = SparkSession.builder \
        .appName("MySQL and Cassandra Integration") \
        .config("spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0") \
        .getOrCreate()
    return spark

def read_from_cassandra(spark):
    tracking = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="tracking", keyspace="study_de") \
        .load()
    return tracking

def clean_cassandra_data(tracking):
    df = tracking.select("create_time", "job_id", "custom_track", "bid", "campaign_id", "group_id", "publisher_id")
    df = df.filter((df.job_id.isNotNull()) & (df.custom_track.isNotNull()))
    def uuid_to_time(uuid_str):
        return time_uuid.TimeUUID(bytes=uuid.UUID(uuid_str).bytes).get_datetime().strftime('%Y-%m-%d %H:%M:%S')
    uuid_to_time_udf = udf(uuid_to_time, StringType())
    df_clean = df.withColumn("ts", uuid_to_time_udf(col("create_time")))
    return df_clean

def process_click_data(df_clean, spark):
    click_data = df_clean.filter(df_clean.custom_track == 'click')
    click_data = df_clean.na.fill({'bid':0})
    click_data = df_clean.na.fill({'job_id':0})
    click_data = df_clean.na.fill({'publisher_id':0})
    click_data = df_clean.na.fill({'group_id':0})
    click_data = df_clean.na.fill({'campaign_id':0})

    click_data.createTempView('click')
    click_output = spark.sql(""" select job_id,date(ts) as Date , hour(ts) as Hour , ts, publisher_id , campaign_id , group_id, avg(bid) as bid_set , count(*) as clicks , sum(bid) as spend_hour  from click group by job_id,date(ts), hour(ts), publisher_id , campaign_id , group_id, ts""")          
    
    return click_output


def process_conversion_data(df_clean, spark):
    conversion_data = df_clean.filter(df_clean.custom_track == 'conversion')
    conversion_data = conversion_data.na.fill({'bid':0})
    conversion_data = conversion_data.na.fill({'job_id':0})
    conversion_data = conversion_data.na.fill({'publisher_id':0})
    conversion_data = conversion_data.na.fill({'group_id':0})
    conversion_data = conversion_data.na.fill({'campaign_id':0})

    conversion_data.createTempView('conversion')
    conversion_output = spark.sql(""" select job_id, date(ts) as Date, hour(ts) as Hour, ts, publisher_id, campaign_id, group_id, count(*) as conversion from conversion group by job_id,date(ts), hour(ts), publisher_id , campaign_id , group_id, ts """)

    return conversion_output


def process_qualified_data(df_clean, spark):
    qualified_data = df_clean.filter(df_clean.custom_track == 'qualified')
    qualified_data = qualified_data.na.fill({'bid':0})
    qualified_data = qualified_data.na.fill({'job_id':0})
    qualified_data = qualified_data.na.fill({'publisher_id':0})
    qualified_data = qualified_data.na.fill({'group_id':0})
    qualified_data = qualified_data.na.fill({'campaign_id':0})

    qualified_data.createTempView('qualified')
    qualified_output = spark.sql(""" select job_id, date(ts) as Date, hour(ts) as Hour, ts, publisher_id, campaign_id, group_id, count(*) as qualified from qualified group by job_id,date(ts), hour(ts), publisher_id , campaign_id , group_id, ts """)
    
    return qualified_output


def process_unqualified_data(df_clean, spark):
    unqualified_data = df_clean.filter(df_clean.custom_track == 'unqualified')
    unqualified_data = unqualified_data.na.fill({'bid':0})
    unqualified_data = unqualified_data.na.fill({'job_id':0})
    unqualified_data = unqualified_data.na.fill({'publisher_id':0})
    unqualified_data = unqualified_data.na.fill({'group_id':0})
    unqualified_data = unqualified_data.na.fill({'campaign_id':0})
    
    unqualified_data.createTempView('unqualified')
    unqualified_output = spark.sql(""" select job_id, date(ts) as Date, hour(ts) as Hour, ts, publisher_id, campaign_id, group_id, count(*) as unqualified from unqualified group by job_id,date(ts), hour(ts), publisher_id , campaign_id , group_id, ts """)
    
    return unqualified_output


def process_final_data(click_output, conversion_output, qualified_output, unqualified_output):
    result = click_output.join(conversion_output,['job_id','date','hour','publisher_id','campaign_id','group_id','ts'],'full') \
        .join(qualified_output,['job_id','date','hour','publisher_id','campaign_id','group_id','ts'],'full') \
        .join(unqualified_output,['job_id','date','hour','publisher_id','campaign_id','group_id','ts'],'full')

    return result


def process_cassandra_data(df_clean, spark):
    #process to desired output
    clicks_output = process_click_data(df_clean, spark)
    conversion_output = process_conversion_data(df_clean, spark)
    qualified_output = process_qualified_data(df_clean, spark)
    unqualified_output = process_unqualified_data(df_clean, spark)
    cassandra_processed = process_final_data(clicks_output,conversion_output,qualified_output,unqualified_output)
    return cassandra_processed


def retrieve_company_data(spark):
    SQL = """(SELECT id as job_id, company_id, group_id, campaign_id FROM job) test"""
    company = spark.read \
    .format("jdbc") \
    .option("url", URL) \
    .option("driver", DRIVER) \
    .option("dbtable", SQL) \
    .option("user", USER) \
    .option("password", PASSWORD) \
    .load()
    return company 
    

def import_to_mysql(cassandra_processed, company):
    final_output = cassandra_processed.join(company,'job_id','left').drop(company.campaign_id).drop(company.group_id)
    final_output = final_output.select('job_id','date','hour','publisher_id','company_id','campaign_id','group_id','unqualified','qualified','conversion','clicks','bid_set','spend_hour','ts')
    final_output = final_output.withColumnRenamed('date','dates') \
        .withColumnRenamed('hour','hours') \
        .withColumnRenamed('qualified','qualified_application') \
        .withColumnRenamed('unqualified','disqualified_application')
    final_output = final_output.withColumn('sources',lit('Cassandra'))
    final_output = final_output.withColumnRenamed('ts','latest_update_time')
    final_output.printSchema()
    final_output.write \
        .format("jdbc") \
        .option("url", URL) \
        .option("driver", DRIVER) \
        .option("dbtable", "events") \
        .option("user", USER) \
        .option("password", PASSWORD) \
        .mode("append") \
        .save()

    return print('Data imported successfully')


def get_mysql_latest_time(spark):
    try:
        sql = """(select max(latest_update_time) from events) data"""
        mysql_time = spark.read.format('jdbc').options(url=URL, driver=DRIVER, dbtable=sql, user=USER, password=PASSWORD).load()
        mysql_time = mysql_time.take(1)[0][0]
        if mysql_time is None:
            mysql_latest = '1998-01-01 23:59:59'
        else:
            mysql_latest = datetime.datetime.strptime(mysql_time, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        mysql_latest = '1998-01-01 23:59:59'
    
    return mysql_latest

def main_task(mysql_time):
    # Step 1: Create Spark session
    spark = create_spark_session()

    # Step 2: Read data from Cassandra
    tracking_data = read_from_cassandra(spark)
    tracking_data = tracking_data.filter(col('ts')>= mysql_time)

    # Step 3: Clean the Cassandra data
    df_clean = clean_cassandra_data(tracking_data)

    # Step 4: Process different types of data from Cassandra
    cassandra_processed = process_cassandra_data(df_clean, spark)

    # Step 5: Retrieve company data from MySQL
    company_data = retrieve_company_data(spark)

    # Step 6: Import processed data to MySQL
    import_to_mysql(cassandra_processed, company_data)

    spark.stop()
    print("Main task completed successfully.")

# Run the main task
if __name__ == "__main__":

    spark = create_spark_session()
    mysql_time = get_mysql_latest_time(spark)

    main_task(mysql_time)

