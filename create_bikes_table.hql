DROP TABle default.bikes;

CREATE EXTERNAL TABLE IF NOT EXISTS default.bikes (trip_duration DOUBLE, start_time STRING, stop_time STRING, start_station_id INT, start_station_name STRING, start_station_latitude DOUBLE, start_station_longitude DOUBLE, end_station_id INT, end_station_name STRING, end_station_latitude DOUBLE, end_station_longitude DOUBLE, bike_id INT, user_type STRING, birth_year DOUBLE, gender INT, start_round_hour STRING, stop_round_hour STRING)
Partitioned By(partition_year STRING, partition_month STRING) 
row format delimited
fields terminated by ','
STORED AS textfile
Location '/user/vagrant/project/data/bikes';
MSCK Repair Table default.bikes;

