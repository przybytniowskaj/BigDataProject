import sys
import logging
import geopandas as gpd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from pyspark.sql.functions import when, col

formatter = logging.Formatter('[%(asctime)s] %(levelname)s @ line %(lineno)d: %(message)s')
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)

def main():
    #findspark.init()
    spark = SparkSession.builder.appName("BikesAnalysis").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("Starting spark application")
    logger.info("Reading CSV File")

    logger.info("Wczytywanie danych z hive...")
    bikes = spark.sql(f"SELECT * FROM bikes where ride_id != 'ride_id'")

    
    logger.info("Analiza 1: Miesiąc a ilość przejazdów")
    monthly_ride_counts = bikes.groupBy("partition_month", "partition_year") \
                                   .count() \
                                   .orderBy("partition_month")
    monthly_ride_counts.show(truncate=False)


    logger.info("Analiza 2: Średnia liczba przejazdów dla dni tygodnia")
    bikes_with_date = bikes.withColumn("date", F.to_date("start_round_hour"))
    daily_trip_counts = bikes_with_date.groupBy("date").count().orderBy("date")
    bikes_with_weekday = daily_trip_counts.withColumn("weekday", F.dayofweek("date"))
    average_trips_by_weekday = bikes_with_weekday.groupBy("weekday") \
                                             .agg(F.avg("count").alias("average_trips")) \
                                             .orderBy("weekday")
    average_trips_by_weekday = average_trips_by_weekday.withColumn("average_trips", F.round("average_trips", 1))
    daily_average_rides = average_trips_by_weekday.withColumn("weekday_name",
                                                            F.when(F.col("weekday") == 1, "Sunday")
                                                            .when(F.col("weekday") == 2, "Monday")
                                                            .when(F.col("weekday") == 3, "Tuesday")
                                                            .when(F.col("weekday") == 4, "Wednesday")
                                                            .when(F.col("weekday") == 5, "Thursday")
                                                            .when(F.col("weekday") == 6, "Friday")
                                                            .when(F.col("weekday") == 7, "Saturday")
                                                            .otherwise("Unknown"))
    daily_average_rides.show(truncate=False)


    
    logger.info("Analiza 3: Summer street w NY")
    bikes_august = bikes_with_date.filter(F.month("start_round_hour") == 8)
    average_trips_august = bikes_august.groupBy("date") \
                                             .count() \
                                             .agg(F.avg("count").alias("average_trips"))
    logger.info("Najpierw średnia ilośc przejazdów w sierpniu")
    average_trips_august.show()
    logger.info("A teraz dla dni summer streets")
    specific_dates = ["2021-08-07", "2021-08-14", "2022-08-05", "2022-08-12", "2022-08-19"]
    specific_dates_df = spark.createDataFrame([(date,) for date in specific_dates], ["specific_date"])
    rides_count_for_specific_dates = bikes.join(specific_dates_df, on=F.date_format("start_round_hour", "yyyy-MM-dd") == F.col("specific_date"), how="right") \
                                                  .groupBy("specific_date") \
                                                  .count() \
                                                  .orderBy("specific_date")
    rides_count_for_specific_dates.show()


    logger.info("Analiza 4: Długość przejazdu a miesiąc w roku")
    bikes_trip_duration = bikes.withColumn("trip_duration", 
                                            (F.unix_timestamp("stop_time") - F.unix_timestamp("start_time")) / 60)
    average_trip_time_per_month = bikes_trip_duration.groupBy("partition_month", "partition_year") \
                                             .agg(F.avg("trip_duration").alias("avg_trip_duration_minutes")) \
                                             .orderBy("partition_month")
    average_trip_time_per_month.show(truncate=False)


    logger.info("Analiza 5: Pory dnia a dzielnica startu")
    boroughs_gdf = gpd.read_file("/home/vagrant/project/borough.geojson")
    @udf(StringType())
    def determine_borough(lat, lng):
        if lat is None or lng is None:
            return None
        
        point = Point(float(lng), float(lat))
        
        
        for idx, row in boroughs_gdf.iterrows():
            if point.within(row['geometry']):
                return row['boro_name']
        return None
    bikes_with_boroughs = bikes.withColumn("start_borough", determine_borough("start_lat", "start_lng"))
    bikes_with_boroughs = bikes_with_boroughs.filter(F.col("start_borough").isNotNull())
    bikes_day_parts = bikes_with_boroughs.withColumn(
        "time_of_day",
        when((col("start_hour") >= 0) & (col("start_hour") < 6), "early_morning")
        .when((col("start_hour") >= 6) & (col("start_hour") < 12), "morning")
        .when((col("start_hour") >= 12) & (col("start_hour") < 18), "afternoon")
        .otherwise("evening")
    )
    borough_hourly_counts = bikes_day_parts.groupBy("start_borough", "start_hour") \
                                           .count() \
                                           .orderBy("start_borough", "start_hour")
    borough_hourly_counts = bikes_day_parts.groupBy("start_borough", "time_of_day").count().orderBy("start_borough", "time_of_day")
    borough_hourly_counts.show(truncate=False)


    logger.info("Analiza 6: Ilość przejazdów dla różnych typów rowerów")
    rideable_count_bikes = bikes.groupBy('rideable_type').count().orderBy('count', ascending=False)
    rideable_count_bikes.show(truncate=False)

    
    logger.info("Ending spark application")
    spark.stop()
    return None


if __name__ == "__main__":
    main()
    sys.exit()
