import psutil
import mysql.connector

# Connect to the ClickHouse database
cnx = mysql.connector.connect(user='<username>', password='<password>', host='<hostname>', port='<port>', database='<database>')
cursor = cnx.cursor()

def monitor_memory_usage():
    for proc in psutil.process_iter():
        try:
            # Get process details as a named tuple
            process = proc.as_dict(attrs=['pid', 'name', 'memory_info'])
            # Get memory usage
            memory_info = process['memory_info']
            # Get process name
            process_name = process['name']
            # Get process id
            process_id = process['pid']
            # Insert process details into ClickHouse database
            query = f"INSERT INTO process_memory_usage (process_name, process_id, memory_usage) VALUES ('{process_name}', {process_id}, {memory_info.rss})"
            cursor.execute(query)
            cnx.commit()
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass

while True:
    monitor_memory_usage()

# Close the cursor and connection
cursor.close()
cnx.close()
