CREATE OR REPLACE TABLE measurements AS
WITH min_max_hours_all_tables AS (
    SELECT MIN(hour) as min_hour, MAX(hour) as max_hour
    FROM measurements_atmospheric_pressure
    UNION ALL
    SELECT MIN(hour) as min_hour, MAX(hour) as max_hour
    FROM measurements_evaporation_tank
    UNION ALL
    SELECT MIN(hour) as min_hour, MAX(hour) as max_hour
    FROM measurements_pluviometer
    UNION ALL
    SELECT MIN(hour) as min_hour, MAX(hour) as max_hour
    FROM measurements_relative_humidity
    UNION ALL
    SELECT MIN(hour) as min_hour, MAX(hour) as max_hour
    FROM measurements_reservoir
    UNION ALL
    SELECT MIN(hour) as min_hour, MAX(hour) as max_hour
    FROM measurements_river
    UNION ALL
    SELECT MIN(hour) as min_hour, MAX(hour) as max_hour
    FROM measurements_snowmeter
    UNION ALL
    SELECT MIN(hour) as min_hour, MAX(hour) as max_hour
    FROM measurements_solar_radiation
    UNION ALL
    SELECT MIN(hour) as min_hour, MAX(hour) as max_hour
    FROM measurements_temperature
    UNION ALL
    SELECT MIN(hour) as min_hour, MAX(hour) as max_hour
    FROM measurements_wind_direction
    UNION ALL
    SELECT MIN(hour) as min_hour, MAX(hour) as max_hour
    FROM measurements_wind_speed
),
min_max_hours AS (
    SELECT MIN(min_hour) AS min_hour, MAX(max_hour) AS max_hour
    FROM min_max_hours_all_tables
),
all_hours AS (
    SELECT UNNEST(GENERATE_SERIES(
        min_hour, 
        max_hour, 
        INTERVAL 1 HOUR
    )) AS hour
    FROM min_max_hours
),
stations_hours AS (
    SELECT st.id AS station_id,
            st.name AS station_name,
            h.hour AS hour
    FROM stations st, all_hours h
)
SELECT sh.station_id,
        sh.station_name,
        sh.hour,
        met.sensor_id AS et_sensor_id,
        met.level_mm AS et_level_mm,
        met.error_pct AS et_error_pct,
        mr.sensor_id AS res_sensor_id,
        mr.level_masl AS res_level_masl,
        mr.volume_hm3 AS res_volume_hm3,
        mt.sensor_id AS tmp_sensor_id,
        mt.temperature_degc AS tmp_degc,
        mt.error_pct AS tmp_error_pct,
        map.sensor_id AS ap_sensor_id,
        map.atmospheric_pressure_mb AS ap_mb,
        map.error_pct AS ap_error_pct,
        mrh.sensor_id AS rh_sensor_id,
        mrh.relative_humidity_pct AS rh_pct,
        mrh.error_pct AS rh_error_pct,
        mws.sensor_id AS ws_sensor_id,
        mws.wind_speed_kmh AS ws_kmh,
        mws.error_pct AS ws_error_pct,
        mwd.sensor_id AS wd_sensor_id,
        mwd.wind_direction_deg AS wd_deg,
        mwd.error_pct AS wd_error_pct,
        msr.sensor_id AS sr_sensor_id,
        msr.solar_radiation_wm2 AS sr_wm2,
        msr.error_pct AS sr_error_pct,
        ms.sensor_id AS sn_sensor_id,
        ms.accumulated_snowfall_lm2 AS sn_lm2,
        mp.sensor_id AS pr_sensor_id,
        mp.accumulated_rain_lm2 AS pr_lm2,
        mrv.sensor_id AS rv_sensor_id,
        mrv.level_m AS rv_level_m,
        mrv.error_pct AS rv_error_pct
FROM stations_hours sh
        LEFT OUTER JOIN measurements_evaporation_tank met
        ON sh.station_id = met.station_id AND
            sh.hour = met.hour
        LEFT OUTER JOIN measurements_reservoir mr
        ON sh.station_id = mr.station_id AND
            sh.hour = mr.hour
        LEFT OUTER JOIN measurements_temperature mt
        ON sh.station_id = mt.station_id AND
            sh.hour = mt.hour
        LEFT OUTER JOIN measurements_atmospheric_pressure map
        ON sh.station_id = map.station_id AND
            sh.hour = map.hour
        LEFT OUTER JOIN measurements_relative_humidity mrh
        ON sh.station_id = mrh.station_id AND
            sh.hour = mrh.hour
        LEFT OUTER JOIN measurements_wind_speed mws
        ON sh.station_id = mws.station_id AND
            sh.hour = mws.hour
        LEFT OUTER JOIN measurements_wind_direction mwd
        ON sh.station_id = mwd.station_id AND
            sh.hour = mwd.hour
        LEFT OUTER JOIN measurements_solar_radiation msr
        ON sh.station_id = msr.station_id AND
            sh.hour = msr.hour
        LEFT OUTER JOIN measurements_snowmeter ms
            ON sh.station_id = ms.station_id AND
            sh.hour = ms.hour
        LEFT OUTER JOIN measurements_pluviometer mp
        ON sh.station_id = mp.station_id AND
            sh.hour = mp.hour
        LEFT OUTER JOIN measurements_river mrv
        ON sh.station_id = mrv.station_id AND
            sh.hour = mrv.hour
ORDER BY sh.hour, sh.station_id;