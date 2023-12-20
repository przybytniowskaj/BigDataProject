DROP TABLE weather;

CREATE EXTERNAL TABLE IF NOT EXISTS default.weather (name STRING, datetime STRING, temp DOUBLE, feelslike DOUBLE, dew DOUBLE, humidity DOUBLE, precip DOUBLE, precipprob DOUBLE, preciptype STRING, snow DOUBLE, snowdepth DOUBLE, windgust DOUBLE, windspeed DOUBLE, winddir DOUBLE, sealevelpressure DOUBLE, cloudcover DOUBLE, visibility DOUBLE, solarradiation DOUBLE, solarenergy DOUBLE, uvindex INT, severerisk STRING, conditions STRING, icon STRING)
Partitioned By(partition_year STRING, partition_month STRING) 
row format delimited
fields terminated by ';'
STORED AS textfile
Location '/user/vagrant/project/data/weather';
MSCK Repair Table default.weather;
