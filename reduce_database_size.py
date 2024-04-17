import os
import duckdb
import shutil
from datetime import datetime

# Backup database
now = datetime.now()
dt_string = now.strftime("%Y%m%d_%H%M%S")
print("Creating a database backup...")
shutil.copy('db/saih-explorer.db', 'db/backup/saih-explorer_' + dt_string + '.db')

# Export database
print("Exporting a database dump...")
con = duckdb.connect('db/saih-explorer.db') # has big file size
con.execute("EXPORT DATABASE 'db/db-dump';")
con.close()

# Remove database
print("Removing existing database...")
os.remove('db/saih-explorer.db')

# Recreate database
print("Creating a new database...")
con = duckdb.connect('db/saih-explorer.db')

# Import data from the dump
print("Importing from the dump files...")
con.execute("IMPORT DATABASE 'db/db-dump';")
con.close()

# Delete dump
print("Deleting the dump files...")
shutil.rmtree('db/db-dump')

print("Process completed.")