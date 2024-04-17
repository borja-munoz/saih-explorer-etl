import duckdb
import os
import progressbar

def get_measurements_table(file_name):
    sensor_type = file_name.split("_")[1][3]
    table_name = ''
    if sensor_type == 'D':
        table_name = 'measurements_evaporation_tank'
    elif sensor_type == 'E' or sensor_type == 'L':
        table_name = 'measurements_reservoir'
    elif sensor_type == 'J' or sensor_type == 'K':
        table_name = ''
    elif sensor_type == 'M':
        sensor_subtype = file_name.split("_")[1][4:6]
        if sensor_subtype == '02':
            table_name = 'measurements_temperature'
        elif sensor_subtype == '03':    
            table_name = 'measurements_atmospheric_pressure'
        elif sensor_subtype == '04':    
            table_name = 'measurements_relative_humidity'
        elif sensor_subtype == '05':    
            table_name = 'measurements_wind_speed'
        elif sensor_subtype == '06':    
            table_name = 'measurements_wind_direction'
        elif sensor_subtype == '07':    
            table_name = 'measurements_solar_radiation'
    elif sensor_type == 'N':
        table_name = 'measurements_snowmeter'
    elif sensor_type == 'P':
        table_name = 'measurements_pluviometer'
    elif sensor_type == 'R':
        table_name = 'measurements_river'
    return table_name

directory = os.fsencode("csv")

with duckdb.connect("db/hidrozarr.db") as con:
    for file in progressbar.progressbar(os.listdir(directory)):
        file_name = os.fsdecode(file)
        table_name = get_measurements_table(file_name)
        if (
            table_name == 'measurements_pluviometer' or
            table_name == 'measurements_snowmeter' 
        ):
            timestamp_format = '%d/%m/%Y %H:%M'
        else:
            timestamp_format = '%d/%m/%y %H:%M'
        if table_name != '' and os.stat('csv/' + file_name).st_size != 0:
            con.sql(f"""
                COPY {table_name} 
                FROM 'csv/{file_name}' 
                (
                    DELIMITER ';', 
                    HEADER, 
                    DECIMAL_SEPARATOR ',',
                    TIMESTAMPFORMAT '{timestamp_format}',
                    NULLSTR 'n/d'
                )
            """)


