import duckdb
import progressbar
import requests

url = 'http://www.redhidrosurmedioambiente.es/saih/datos/a/la/carta/parametros'
url = 'http://www.redhidrosurmedioambiente.es/saih/datos/a/la/carta/csv?'
url += 'agrupacion=60'  # Hourly data
start_year = 2010
end_year = 2012

with duckdb.connect("db/hidrozarr.db") as con:
    sensors = con.sql("""
        SELECT se.id, se.station_id
        FROM stations st 
        INNER JOIN sensors se on st.id = se.station_id
        ORDER BY se.station_id
    """).fetchall()
    for sensor in progressbar.progressbar(sensors):
        station_id = sensor[1]
        sensor_id = sensor[0]
        for year in range(2012, 2024, 2):
            start_date = '01/01/' + str(year)
            end_date = '31/12/' + str(year + 1)
            response = requests.get(
                url + 
                f"&datepickerini={start_date + ' 00:00'}" + 
                f"&datepickerfin={end_date + ' 23:00'}"
                f"&estacion={station_id}" + 
                f"&sensor={sensor_id}"
            )
            file_name = f"csv/{station_id}_{sensor_id}"
            file_name += f"_{start_date.replace('/', '-')}"
            file_name += f"_{end_date.replace('/', '-')}"
            file_name += ".csv"
            with open(file_name, "w") as f:
                # The first 43 characters must be removed because they contain
                # the following text:
                # La consulta no puede ser superior a 31 días
                f.write(response.text[43:])
