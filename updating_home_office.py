# -- PREPARED BY: NUR FARZANA
# -- DATE: 2021-5-12
# -- TEAM: DATA ANALYTICS TEAM
# -- POSITION: DATA ANALYST
# Description: Home/office/high-frequency places(hf-places) location identification
# coding: utf-8
# s3://xxx-business-insights/prod/affluence/script/home_office_hfplaces_location_v2.py



# INFORMATION PROVIDED HERE HAS BEEN ALTERED DUE TO PRIVACY AND CONFIDENTIAL REASON, 
# THIS SCRIPT IS ONLY TO GIVE EXPOSURE TO HIRING MANAGERS ON HISTORICAL PROJECT I HAVE DONE

# THIS PYSPARK SCRIPT IS RUN IN PYSPAKR ENVIRONMENT, AWS


pq = spark.read.parquet("s3a://xxx-prod-data/etl/data/brq/raw/mw/daily/MY/20211108/part-00000-9161d69a-df1d-4b73-881f-d9e839b94edc-c000.snappy.parquet")
daily = spark.read.parquet("s3a://xxx-prod-data/etl/data/brq/agg/agg_brq/daily/MY/20211031/part-00000-0f701f94-6580-45d1-8613-1d07f2b34c42-c000.snappy.parquet")

timeseries = spark.read.parquet("s3a://xxx-prod-data/etl/data/brq/agg/agg_brq/timeseries/daily/MY/20210425/part-00000-ca4948f2-1aaf-47c8-9bc2-ebebdf6ae6cc-c000.snappy.parquet")




# home:
# spark-submit --packages org.apache.spark:spark-avro_2.11:2.4.3,org.apache.hadoop:hadoop-aws:2.7.1  --driver-memory 10g --executor-memory 10g --conf spark.driver.maxResultSize=0 --master local[*] home_office_location_v2.py --input_data_path s3a://xxx-prod-data/etl/data/brq/agg/agg_brq/timeseries/daily/{}/{}*/ --month 202106 --country MY --result_path s3a://xxx-business-insights/prod/affluence/{}_location/{}/{} --agg_type home

# office:
# spark-submit --packages org.apache.spark:spark-avro_2.11:2.4.3,org.apache.hadoop:hadoop-aws:2.7.1  --driver-memory 10g --executor-memory 10g --conf spark.driver.maxResultSize=0 --master local[*] home_office_location_v2.py --input_data_path s3a://xxx-prod-data/etl/data/brq/agg/agg_brq/timeseries/daily/{}/{}*/ --month 202106 --country MY --result_path s3a://xxx-business-insights/prod/affluence/{}_location/{}/{} --agg_type office
# hf-places:
# spark-submit --packages org.apache.spark:spark-avro_2.11:2.4.3,org.apache.hadoop:hadoop-aws:2.7.1  --driver-memory 10g --executor-memory 10g --conf spark.driver.maxResultSize=0 --master local[*] home_office_location_v2.py --input_data_path s3a://xxx-prod-data/etl/data/brq/agg/agg_brq/timeseries/daily/{}/{}*/ --month 202106 --country MY --result_path s3a://xxx-business-insights/prod/affluence/{}_location/{}/{} --agg_type hf-places


from pyspark.sql.window import Window
from pyspark.sql.functions import *

from pyspark.sql import SparkSession
from pyspark.sql.types import *

import os

# importing PySpark packages/APIs
import sys
import argparse
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession

import configparser
import os

from functools import reduce
from pyspark.sql import DataFrame

import time
from datetime import datetime
from dateutil.relativedelta import relativedelta

def setup_aws_credential(aws_profile):
    #
    # Read AWS
    #
    import configparser, os
    global spark
    #
    config = configparser.ConfigParser()
    config.read(os.path.expanduser("~/.aws/credentials"))
    access_id = config.get(aws_profile, "aws_access_key_id")
    access_key = config.get(aws_profile, "aws_secret_access_key")
    aws_session_token = config.get(aws_profile, "aws_session_token")
    #
    # Hadoop configuration
    #
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    hadoop_conf.set("fs.s3a.access.key", access_id)
    hadoop_conf.set("fs.s3a.secret.key", access_key)
    hadoop_conf.set("fs.s3a.session.token", aws_session_token)
    hadoop_conf.set("fs.s3a.fast.upload", "true")
    hadoop_conf.set("fs.s3a.buffer.dir", "/tmp")



if __name__ == '__main__':


    parser = argparse.ArgumentParser(description='xxx - connection segments data set')

    parser.add_argument(
            '--input_data_path',
            required=True,
            type=str,
            help='s3a://xxx-prod-data/etl/data/brq/agg/agg_brq/timeseries/daily/MY/20210425/part-00000-ca4948f2-1aaf-47c8-9bc2-ebebdf6ae6cc-c000.snappy.parquet')

    parser.add_argument(
            '--month',
            required=True,
            type=str,
            help='--month 202106')

    parser.add_argument(
            '--country',
            required=True,
            type=str,
            help='--country MY')

    parser.add_argument(
            '--result_path',
            required=True,
            type=str,
            help='s3a://xxx-dev/farzanaamir/home_location/output/{}')

    parser.add_argument(
            '--agg_type',
            required=True,
            type=str,
            help='home/office')

    parser.add_argument('--aws_profile',
        nargs ='?',
        const = 'default',
        type = str,
        help = 'Specify the aws_profile if needed. '\
               'Example: --aws_profile default')


    args = parser.parse_args()
    input_data_path = args.input_data_path
    month = args.month
    country = args.country
    result_path = args.result_path
    agg_type = args.agg_type
    aws_profile = args.aws_profile



    spark = SparkSession.builder.appName('home_office_hfplaces_location.py').getOrCreate()

    if aws_profile:
        setup_aws_credential(aws_profile)


    start_time = time.time()

    input_dfs = []
    month = '202106'
    date_obj = datetime.strptime(month,"%Y%m")


    ## look-back period 6 months, and create the union dataframe
for i in range(1,1 + 6):
    pre_date_obj = date_obj - relativedelta(months=i)
    pre_month_str = pre_date_obj.strftime("%Y%m")

        final_input_data_path = input_data_path.format(country,pre_month_str)
        print(final_input_data_path)

        input_data = spark.read.parquet(final_input_data_path)
        input_dfs.append(input_data)

    union_input_data = reduce(DataFrame.unionAll, input_dfs)


    ## explode hourly lat/lon track and signal count
    input_data_flat = union_input_data.select("ifa",F.explode(F.col("gps")).alias("gps")).select("ifa","gps.geohash","gps.latitude","gps.longitude","gps.date",
                                                "gps.hour_00","gps.hour_01","gps.hour_02","gps.hour_03","gps.hour_04",
                                                "gps.hour_05","gps.hour_06","gps.hour_07","gps.hour_08","gps.hour_09",
                                                "gps.hour_10","gps.hour_11","gps.hour_12","gps.hour_13","gps.hour_14",
                                                "gps.hour_15","gps.hour_16","gps.hour_17","gps.hour_18","gps.hour_19",
                                                "gps.hour_20","gps.hour_21","gps.hour_22","gps.hour_23").cache()


    ## clean-up latitude and longitude with rule of thumb (day,lat,lon,ifa >1)
    input_data_flat_count = input_data_flat.groupBy("date","latitude","longitude").agg(countDistinct("ifa").alias("count"))
    input_data_valid_lat_lon = input_data_flat_count.filter(F.col("count") <= 1)

    input_data_flat_filtered = input_data_valid_lat_lon.join(input_data_flat, on = ['date','latitude','longitude'])


    ## round the latitude and longitude with 2 decimals
    input_data_flat_rounded = input_data_flat_filtered.withColumn("latitude",F.round("latitude",2)).withColumn("longitude",F.round("longitude",2))


    ##  get the top lat lon for each hour
    dfs = []

    for c in input_data_flat_rounded.columns:
        if c.startswith("hour"):

            hour = int(c.replace("hour_",""))
            input_data_flat_rounded_filtered = input_data_flat_rounded.filter(F.col(c) >0 )
            ### agg brq count by hour
            group_df = input_data_flat_rounded_filtered.groupBy("ifa","date","latitude","longitude").agg(sum(c).alias("hour_brq_count"))
            ### get the highest brq count for each hour
            rank_df = group_df.withColumn("rank",dense_rank().over(Window.partitionBy("ifa","date").orderBy(desc("hour_brq_count"))))
            max_df = rank_df.filter(F.col("rank") == 1)
            ### create new column and impute with hour
            hour_df = max_df.select("ifa",'date',"latitude","longitude","hour_brq_count").withColumn("hour",F.lit(hour))
            dfs.append(hour_df)




    ## union all the hour's dataframe
    union_hours_df = reduce(DataFrame.unionAll, dfs)
    ## calcuate day of week
    union_hours_df = union_hours_df.withColumn("dow",F.dayofweek(F.to_date("date","yyyyMMdd"))) ## weekofday starts from Sunday as 1





    ## filter out by the specific timeframe
    if agg_type == 'home':
        union_hours_df_frame = union_hours_df.filter(F.col("hour").isin(21,22,23,0,1,2,3,4,5,6,7))
    elif agg_type == 'office':
        union_hours_df = union_hours_df.filter(~F.col("dow").isin(1,7)) ## exclude Sat and Sun
        union_hours_df_frame = union_hours_df.filter(F.col("hour").isin(10,11,12,13,14,15,16,17))
    elif agg_type == 'hf-places':
        union_hours_df_frame = union_hours_df

    ## get the top brq-count lat/lon within the timeframe for each day
    group_df = union_hours_df_frame.groupBy("ifa","latitude","longitude","date").agg(sum("hour_brq_count").alias("time_frame_count"))

    rank_df = group_df.withColumn("rank",row_number().over(Window.partitionBy("ifa","date").orderBy(desc("time_frame_count"))))
    max_df = rank_df.filter(F.col("rank") == 1)


    ## calculate the elapse days for the captured date
    # -> to solve the issue if multiple lat/lon points have the same day-counts in next stage, elapse days will be used as an extra factor
    max_df = max_df.withColumn("date_dt",to_date(F.col("date"),"yyyyMMdd"))
    max_df = max_df.withColumn("elapsed_days",datediff(current_date(),col("date_dt")).alias("datediff"))


    ## get the top lat/lon day-counts across the given period - 6 months, as well as average elapsed days for each lat/lon point
    timeframe_df = max_df.groupBy("ifa","latitude","longitude").agg(countDistinct("date").alias("days_count"),sum("elapsed_days").alias("elapsed_days_total"))

    timeframe_df = timeframe_df.withColumn("elapsed_days_avg",F.col("elapsed_days_total")/F.col("days_count"))
    ## rank by average elapsed days and days count
    timeframe_df_rank = timeframe_df.withColumn("rank",row_number().over(Window.partitionBy("ifa").orderBy("elapsed_days_avg",desc("days_count"))))

    ## for high-frequency places retain 3 top lat/lons, for home/office, retain top 1
    if agg_type == 'hf-places':
        print("hf-places, top 3")
        timeframe_df_top = timeframe_df_rank.filter(F.col("rank") <= 3 )
    else:
        print("home/office, top 1")
        timeframe_df_top = timeframe_df_rank.filter(F.col("rank") == 1 )

    # timeframe_df_top.write.csv(result_path.format(agg_type,month),mode = "overwrite",header = True)
    timeframe_df_top.write.parquet(result_path.format(agg_type,country,month),mode = "overwrite")


    print("--- %s seconds ---" % (time.time() - start_time))
