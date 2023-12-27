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
    return spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="tracking", keyspace="study_de") \
        .load()

def clean_cassandra_data(df):
    df_clean = df.select("create_time", "job_id", "custom_track", "bid", "campaign_id", "group_id", "publisher_id")
    df_clean = df_clean.filter((df_clean.job_id.isNotNull()) & (df_clean.custom_track.isNotNull()))
    def uuid_to_time(uuid_str):
        return time_uuid.TimeUUID(bytes=uuid.UUID(uuid_str).bytes).get_datetime().strftime('%Y-%m-%d %H:%M:%S')
    uuid_to_time_udf = udf(uuid_to_time, StringType())
    return df_clean.withColumn("ts", uuid_to_time_udf(col("create_time")))

def process_click_data(df, spark):
    click_data = df.filter(df.custom_track == 'click')
    click_data = click_data.na.fill({'bid':0})
    click_data = click_data.na.fill({'job_id':0})
    click_data = click_data.na.fill({'publisher_id':0})
    click_data = click_data.na.fill({'group_id':0})
    click_data = click_data.na.fill({'campaign_id':0})
    click_data.createTempView('click')
    return spark.sql(""" select job_id,date(ts) as Date , hour(ts) as Hour , ts, publisher_id , campaign_id , group_id, avg(bid) as bid_set , count(*) as clicks , sum(bid) as spend_hour  from click group by job_id,date(ts), hour(ts), publisher_id , campaign_id , group_id, ts""")          


def process_conversion_data(df, spark):
    conversion_data = df.filter(df.custom_track == 'conversion')
    conversion_data = conversion_data.na.fill({'bid':0})
    conversion_data = conversion_data.na.fill({'job_id':0})
    conversion_data = conversion_data.na.fill({'publisher_id':0})
    conversion_data = conversion_data.na.fill({'group_id':0})
    conversion_data = conversion_data.na.fill({'campaign_id':0})
    conversion_data.createTempView('conversion')
    return spark.sql(""" select job_id, date(ts) as Date, hour(ts) as Hour, ts, publisher_id, campaign_id, group_id, count(*) as conversion from conversion group by job_id,date(ts), hour(ts), publisher_id , campaign_id , group_id, ts """)


def process_qualified_data(df, spark):
    qualified_data = df.filter(df.custom_track == 'qualified')
    qualified_data = qualified_data.na.fill({'bid':0})
    qualified_data = qualified_data.na.fill({'job_id':0})
    qualified_data = qualified_data.na.fill({'publisher_id':0})
    qualified_data = qualified_data.na.fill({'group_id':0})
    qualified_data = qualified_data.na.fill({'campaign_id':0})
    qualified_data.createTempView('qualified')
    return spark.sql(""" select job_id, date(ts) as Date, hour(ts) as Hour, ts, publisher_id, campaign_id, group_id, count(*) as qualified from qualified group by job_id,date(ts), hour(ts), publisher_id , campaign_id , group_id, ts """)


def process_unqualified_data(df, spark):
    unqualified_data = df.filter(df.custom_track == 'unqualified')
    unqualified_data = unqualified_data.na.fill({'bid':0})
    unqualified_data = unqualified_data.na.fill({'job_id':0})
    unqualified_data = unqualified_data.na.fill({'publisher_id':0})
    unqualified_data = unqualified_data.na.fill({'group_id':0})
    unqualified_data = unqualified_data.na.fill({'campaign_id':0})
    unqualified_data.createTempView('unqualified')
    return spark.sql(""" select job_id, date(ts) as Date, hour(ts) as Hour, ts, publisher_id, campaign_id, group_id, count(*) as unqualified from unqualified group by job_id,date(ts), hour(ts), publisher_id , campaign_id , group_id, ts """)


def process_final_data(click_output, conversion_output, qualified_output, unqualified_output):
    return click_output.join(conversion_output,['job_id','date','hour','publisher_id','campaign_id','group_id','ts'],'full') \
        .join(qualified_output,['job_id','date','hour','publisher_id','campaign_id','group_id','ts'],'full') \
        .join(unqualified_output,['job_id','date','hour','publisher_id','campaign_id','group_id','ts'],'full')


def process_cassandra_data(df, spark):
    #process to desired output
    click_output = process_click_data(df, spark)
    conversion_output = process_conversion_data(df, spark)
    qualified_output = process_qualified_data(df, spark)
    unqualified_output = process_unqualified_data(df, spark)
    return process_final_data(click_output,conversion_output,qualified_output,unqualified_output)


def retrieve_company_data(spark):
    SQL = """(SELECT id as job_id, company_id, group_id, campaign_id FROM job) test"""
    return spark.read \
        .format("jdbc") \
        .option("url", URL) \
        .option("driver", DRIVER) \
        .option("dbtable", SQL) \
        .option("user", USER) \
        .option("password", PASSWORD) \
        .load() 
    

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

    print('Data imported successfully')
    return


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

def main_task(spark, mysql_time):
    # Step 1: Read data from Cassandra
    tracking_data = read_from_cassandra(spark)
    tracking_data = tracking_data.filter(col('ts')>= mysql_time)

    # Step 2: Clean the Cassandra data
    df_clean = clean_cassandra_data(tracking_data)

    # Step 3: Process different types of data from Cassandra
    cassandra_processed = process_cassandra_data(df_clean, spark)

    # Step 4: Retrieve company data from MySQL
    company_data = retrieve_company_data(spark)

    # Step 5: Import processed data to MySQL
    import_to_mysql(cassandra_processed, company_data)

    spark.stop()
    print("Main task completed successfully.")

# Run the main task
if __name__ == "__main__":

    spark = create_spark_session()
    mysql_time = get_mysql_latest_time(spark)

    main_task(spark, mysql_time)

