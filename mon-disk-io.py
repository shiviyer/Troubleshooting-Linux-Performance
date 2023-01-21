import psutil
import mysql.connector

# Connect to the ClickHouse database
cnx = mysql.connector.connect(user='<username>', password='<password>', host='<hostname>', port='<port>', database='<database>')
cursor = cnx.cursor()

def monitor_disk_io_usage():
    disk_io = psutil.disk_io_counters()
    # Get read_count
    read_count = disk_io.read_count
    # Get write_count
    write_count = disk_io.write_count
    # Get read_bytes
    read_bytes = disk_io.read_bytes
    # Get write_bytes
    write_bytes = disk_io.write_bytes
    # Insert disk I/O usage into ClickHouse database
    query = f"INSERT INTO disk_io_usage (read_count, write_count, read_bytes, write_bytes) VALUES ({read_count}, {write_count}, {read_bytes}, {write_bytes})"
    cursor.execute(query)
    cnx.commit()

while True:
    monitor_disk_io_usage()

# Close the cursor and connection
cursor.close()
cnx.close()
