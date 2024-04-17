-- The original data is available by station and hour, so we always
-- need to start aggregating from hourly data. The aggregation
-- period will be determined by the time step parameter.

-- The stations need to be filtered, either by the station id,
-- the province or the basin. Right now we only have stations
-- from one basin, so there is no need to filter by basin.

-- Depending on the metric, this aggregation will be a sum of
-- the values of all stations, like in the case of reservoir volumes,
-- or it will be an average, like in the case of temperature.

-- If the selected entity is a station, we just need to average
-- the hourly values. But, if it is a province or a basin, then
-- we need to aggregate the measurements from all the stations
-- in the provicnce or basin.

-- For metrics that are averaged, we don't need to do anything else.
-- For metrics that are summed up, we need to have two aggregations:
-- we first sum up the hourly measurements and then we average
-- by the measurement period.

-- Station selected, summed up metric:
SELECT AVG(volume_hm3) AS volume_hm3,
       EXTRACT('year' FROM DATE_TRUNC('year', hour)) AS year
FROM measurements_reservoir
WHERE station_id = 16
GROUP BY DATE_TRUNC('year', hour) 

-- Province selected, summed up metric:
WITH hourly_data AS (
  SELECT SUM(volume_hm3) AS volume_hm3,
         hour
  FROM measurements_reservoir a
       INNER JOIN stations b ON a.station_id = b.id
  WHERE b.province = 'Málaga'
  GROUP BY hour
)
SELECT AVG(volume_hm3) AS volume_hm3,
       EXTRACT('year' FROM DATE_TRUNC('year', hour)) AS year
FROM hourly_data
GROUP BY DATE_TRUNC('year', hour);

-- Basin selected, averaged metric
SELECT AVG(temperature_degc) AS temperature_degc,
       EXTRACT('year' FROM DATE_TRUNC('year', hour)) AS year
FROM measurements_temperature
GROUP BY DATE_TRUNC('year', hour);
