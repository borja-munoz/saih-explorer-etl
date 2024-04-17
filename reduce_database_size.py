import os
import duckdb
import shutil
from datetime import datetime

# Backup database
now = datetime.now()
dt_string = now.strftime("%Y%m%d_%H%M%S")
print("Creating a database backup...")
shutil.copy('db/reservoir-analysis.db', 'db/backup/reservoir-analysis_' + dt_string + '.db')

# Export database
print("Exporting a database dump...")
con = duckdb.connect('db/reservoir-analysis.db') # has big file size
con.execute("EXPORT DATABASE 'db/db-dump';")
con.close()

# Remove database
print("Removing existing database...")
os.remove('db/reservoir-analysis.db')

# Recreate database
# In order to be able to load the spatial extension, it needs
# to be pre-installed in the ~/.duckdb/extensions directory
print("Creating a new database and loading the spatial extension...")
con = duckdb.connect('db/reservoir-analysis.db')
con.execute("INSTALL spatial;")
con.execute("LOAD spatial;")

# Import data from the dump
print("Importing from the dump files...")
con.execute("IMPORT DATABASE 'db/db-dump';")
con.close()

# Delete dump
print("Deleting the dump files...")
shutil.rmtree('db/db-dump')

print("Process completed.")