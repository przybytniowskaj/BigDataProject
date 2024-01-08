import logging
import sys

import findspark
findspark.init()
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import happybase
from tqdm.notebook import tqdm

formatter = logging.Formatter('[%(asctime)s] %(levelname)s @ line %(lineno)d: %(message)s')
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)

class MyConnection:

    def __init__(self, host='localhost'):
        self.connection = happybase.Connection(host, autoconnect=False, port=9090)

    def __enter__(self):
        self.connection.open()
        return self.connection

    def __exit__(self, type, value, traceback):
        self.connection.close()

def create_weather_analysis():
    logger.info("Building spark session... ")
    spark = SparkSession \
    .builder \
    .appName("Load Hive Table") \
    .enableHiveSupport() \
    .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("done")

    logger.info("Collecting data from hive... ")
    bikes = spark.sql("select * from bikes where ride_id !='ride_id'")
    weather = spark.sql("select * from weather where name != 'name'")
    logger.info("done")

    logger.info("Preparing weather data...")
    tmp_weather = weather.withColumn("date", F.to_date('datetime')).groupBy('date').agg({
        'temp': 'mean',
        'precip': 'mean',
        'windspeed': 'mean',
    })\
        .withColumnRenamed("avg(temp)", "avg_temp")\
        .withColumnRenamed("avg(precip)", "avg_precip")\
        .withColumnRenamed("avg(windspeed)", "avg_windspeed")
    weather_avg = tmp_weather\
        .withColumn('avg_temp_bin', F.round(tmp_weather.avg_temp))\
        .withColumn('avg_precip_bin', F.round(tmp_weather.avg_precip * 100) / 100)\
        .withColumn('avg_windspeed_bin', F.round(tmp_weather.avg_windspeed)).select('date', 'avg_temp_bin', 'avg_precip_bin', 'avg_windspeed_bin')
    logger.info("done")

    logger.info("Preparing bikes data...")
    tmp_bikes = bikes.withColumn('start_timestamp', F.unix_timestamp('start_time', format='yyyy-MM-dd HH:mm:ss'))
    tmp_bikes = tmp_bikes.withColumn('stop_timestamp', F.unix_timestamp('stop_time', format='yyyy-MM-dd HH:mm:ss'))
    tmp_bikes = tmp_bikes.withColumn('duration', tmp_bikes.stop_timestamp - tmp_bikes.start_timestamp)
    tmp_bikes = tmp_bikes.withColumn("date", F.to_date('start_time')).groupBy('date')\
        .agg(
            F.count(F.lit(1)).alias("ride_count"),
            F.sum('duration').alias("total_triptime"),
            F.mean('duration').alias("mean_triptime"),
        )
    logger.info("done")
    
    bike_rides = tmp_bikes.select('date', 'ride_count', 'total_triptime', 'mean_triptime')
    weather_avg.join(bike_rides, on='date').createOrReplaceTempView("weather_and_rides")
    
    df = spark.sql("select * from weather_and_rides")
    return df

def save_analysis_to_hbase(df):
    TABLE_NAME = "weather_and_rides"
    with MyConnection() as connection:
        logger.info("checking if the table already exists...")
        if TABLE_NAME.encode() in connection.tables():
            print("droping an existing table...")
            connection.delete_table(TABLE_NAME, disable=True)
        logger.info("creating a new table...")
        connection.create_table(
            TABLE_NAME,
            {'time': dict(),
             'weather': dict(),
             'rides': dict(),  # use defaults
            }
        )
        logger.info("getting table...")
        table = connection.table(TABLE_NAME)
        logger.info("putting data...")
        with tqdm(total=df.shape[0]) as pbar:
            for index, row in df.iterrows():
                pbar.update(1)
                row_key = row['date'].strftime('%Y-%m-%d')[::-1].replace('-', '') # reversed date string
                data = {
                    b'time:date': (f"{row['date'].strftime('%Y-%m-%d')}").encode(),
                    b'weather:avg_temp_bin': (f"{row['avg_temp_bin']}").encode(),
                    b'weather:avg_windspeed_bin': (f"{row['avg_windspeed_bin']}").encode(),
                    b'weather:avg_precip_bin': (f"{row['avg_precip_bin']}").encode(),
                    b'rides:ride_count': (f"{row['ride_count']}").encode(),
                    b'rides:total_triptime': (f"{row['total_triptime']}").encode(),
                    b'rides:mean_triptime': (f"{row['mean_triptime']}").encode(),
                }        
                table.put(row_key.encode(), data)

def main():
    df = create_weather_analysis()
    save_analysis_to_hbase(df.toPandas())

if __name__ == "__main__":
    main()
    sys.exit()