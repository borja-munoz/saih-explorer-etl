import duckdb
import fiona
import requests

url = 'http://www.redhidrosurmedioambiente.es/saih/datos/a/la/carta/parametros'
payload = {
    "agrupacion": "60"
}

response = requests.post(url, payload)
stations = response.json()

print(f"There are {len(stations)} stations")

def read_kml_stations():
    # KML file with station coordinates
    kml_url = 'http://www.redhidrosurmedioambiente.es/saih/assets/Estaciones_SAIH.kml'
    kml_file_name = 'stations.kml'
    response = requests.get(kml_url)
    with open(kml_file_name, "w") as f:
        f.write(response.text)

    # We cannot use directly ST_Read from the DuckDB spatial extension
    # because it does not support 3D coordinates yet (v0.10) and fails
    # with the error "Not implemented Error: Geometry type '1001' not supported"
    # kml_stations = con.sql(f"SELECT * FROM ST_Read('{kml_file_name}')").fetchall()

    # We can read the file using Fiona
    fiona.drvsupport.supported_drivers['KML'] = 'rw'
    colxn = fiona.open(kml_file_name, "r")
    station_coords = {}
    for feature in colxn:
        station_coords[feature.properties['Name']] = feature.geometry['coordinates']

def create_stations_table():

    # Create table 
    con.sql("""
        CREATE OR REPLACE TABLE stations (
            id INT, 
            name VARCHAR, 
            province VARCHAR,
            type_description VARCHAR, 
            x FLOAT,
            y FLOAT,
            z FLOAT,
            PRIMARY KEY (id, name)
        )
    """)

    # Load stations from CSV file
    con.sql("COPY stations FROM 'stations.csv' (DELIMITER ';', HEADER, decimal_separator ',')")

    # Add additional columns
    con.sql("""
        ALTER TABLE STATIONS
        ADD COLUMN type VARCHAR
    """)
    con.sql("""
        ALTER TABLE STATIONS
        ADD COLUMN subsystem VARCHAR
    """)
    con.sql("""
        ALTER TABLE STATIONS
        ADD COLUMN geom_25830 GEOMETRY
    """)
    con.sql("""
        ALTER TABLE STATIONS
        ADD COLUMN geom_4326 GEOMETRY
    """)

def create_sensors_table():

    con.sql("""
        CREATE OR REPLACE TABLE sensors (
            id VARCHAR, 
            station_id INT,
            name VARCHAR,
            PRIMARY KEY (id)
        )
    """)

def create_measurements_tables():
    con.sql("""
        CREATE OR REPLACE TABLE measurements_evaporation_tank (
            station_id INT,
            name VARCHAR,
            sensor_id VARCHAR,
            hour TIMESTAMP,
            sensor_name VARCHAR,
            level_mm FLOAT,
            error_pct FLOAT
        )            
    """)
    con.sql("""
        CREATE OR REPLACE TABLE measurements_reservoir (
            station_id INT,
            name VARCHAR,
            sensor_id VARCHAR,
            hour TIMESTAMP,
            level_masl FLOAT,
            volume_hm3 STRING
        )            
    """)
    con.sql("""
        CREATE OR REPLACE TABLE measurements_temperature (
            station_id INT,
            name VARCHAR,
            sensor_id VARCHAR,
            hour TIMESTAMP,
            temperature_degc FLOAT,
            error_pct FLOAT
        )            
    """)
    con.sql("""
        CREATE OR REPLACE TABLE measurements_atmospheric_pressure (
            station_id INT,
            name VARCHAR,
            sensor_id VARCHAR,
            hour TIMESTAMP,
            atmospheric_pressure_mb FLOAT,
            error_pct FLOAT
        )            
    """)
    con.sql("""
        CREATE OR REPLACE TABLE measurements_relative_humidity (
            station_id INT,
            name VARCHAR,
            sensor_id VARCHAR,
            hour TIMESTAMP,
            relative_humidity_pct FLOAT,
            error_pct FLOAT
        )            
    """)
    con.sql("""
        CREATE OR REPLACE TABLE measurements_wind_speed (
            station_id INT,
            name VARCHAR,
            sensor_id VARCHAR,
            hour TIMESTAMP,
            wind_speed_kmh FLOAT,
            error_pct FLOAT
        )            
    """)
    con.sql("""
        CREATE OR REPLACE TABLE measurements_wind_direction (
            station_id INT,
            name VARCHAR,
            sensor_id VARCHAR,
            hour TIMESTAMP,
            wind_direction_deg FLOAT,
            error_pct FLOAT
        )            
    """)
    con.sql("""
        CREATE OR REPLACE TABLE measurements_solar_radiation (
            station_id INT,
            name VARCHAR,
            sensor_id VARCHAR,
            hour TIMESTAMP,
            solar_radiation_wm2 FLOAT,
            error_pct FLOAT
        )            
    """)
    con.sql("""
        CREATE OR REPLACE TABLE measurements_snowmeter (
            station_id INT,
            name VARCHAR,
            sensor_id VARCHAR,
            hour TIMESTAMP,
            sensor_name VARCHAR,
            accumulated_snowfall_lm2 FLOAT
        )            
    """)
    con.sql("""
        CREATE OR REPLACE TABLE measurements_pluviometer (
            station_id INT,
            name VARCHAR,
            sensor_id VARCHAR,
            hour TIMESTAMP,
            sensor_name VARCHAR,
            accumulated_rain_lm2 FLOAT
        )            
    """)
    con.sql("""
        CREATE OR REPLACE TABLE measurements_river (
            station_id INT,
            name VARCHAR,
            sensor_id VARCHAR,
            hour TIMESTAMP,
            level_m FLOAT,
            error_pct FLOAT
        )            
    """)

with duckdb.connect("db/hidrozarr.db") as con:

    # Install and load the spatial extension
    con.install_extension("spatial")
    con.load_extension("spatial")

    # station_coords = read_kml_stations()

    create_stations_table()
    create_sensors_table()
    create_measurements_tables()

    # Update stations and insert sensors
    for key, value in stations.items():
        con.sql(f"""
            UPDATE stations 
            SET type = '{value["tipoestacion"][0]}', 
                subsystem = '{value["subsistema"]}',
                geom_25830 = ST_Point(x, y),
                geom_4326 = ST_Transform(ST_Point(x, y), 'EPSG:25830', 'EPSG:4326')
            WHERE id = {key}
        """)
        for index, sensor_name in enumerate(value['sensores']):
            con.sql(f"""
                INSERT INTO sensors (id, station_id, name) 
                VALUES (
                    '{sensor_name}',
                    {key}, 
                    '{value["nombres"][index]}'
                )
            """)