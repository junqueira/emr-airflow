from pyhive import hive

conn = hive.connect("localhost", 10000)

cursor = conn.cursor()

cursor.execute("SELECT * FROM dwdii2.noaa_temps LIMIT 100")

print cursor.fetchall()