DROP TABle default.bikes;

CREATE EXTERNAL TABLE IF NOT EXISTS default.bikes (ride_id STRING, rideable_type STRING, start_time STRING, stop_time STRING, start_station_name STRING, start_station_id DOUBLE, end_station_name STRING, end_station_id DOUBLE, start_lat DOUBLE, start_lng DOUBLE, end_lat DOUBLE, end_lng DOUBLE, member_casual STRING, start_round_hour STRING, stop_round_hour STRING)
Partitioned By(partition_year STRING, partition_month STRING) 
row format delimited
fields terminated by ','
STORED AS textfile
Location '/user/vagrant/project/data/bikes';
MSCK Repair Table default.bikes;

