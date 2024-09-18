import mysql.connector
import os
from datetime import datetime


def convert_to_binary_data(filename):
    with open(filename, 'rb') as file:
        binary_data = file.read()
    return binary_data

def insert_blob(file_path):
    try:
        connection = mysql.connector.connect(
            host='127.0.0.1',
            port='3306',
            database='data_engineering_database',
            user='root',
            password='password'
        )

        
        file_stats = os.stat(file_path)

        metadata_string = (
            f"{file_stats.st_size} bytes\n", 
            f"{datetime.fromtimestamp(file_stats.st_ctime).strftime('%Y-%m-%d %H:%M:%S')}\n",
            f"{datetime.fromtimestamp(file_stats.st_mtime).strftime('%Y-%m-%d %H:%M:%S')}\n",
            f"{datetime.fromtimestamp(file_stats.st_atime).strftime('%Y-%m-%d %H:%M:%S')}",
        )

        cursor = connection.cursor()
        sql_insert_blob = """
        INSERT INTO meta_data_info (file_name, file_size, creation_time, last_modification_time, last_access_time, img_data) VALUES (%s, %s, %s, %s, %s, %s)
        """
        binary_data = convert_to_binary_data(file_path)
        cursor.execute(sql_insert_blob, (file_path, metadata_string[0], metadata_string[1], metadata_string[2], metadata_string[3], binary_data,))

        # Commit the transaction
        connection.commit()

        print("Blob inserted successfully")
    
    except mysql.connector.Error as error:
        print(f"Failed to insert blob: {error.msg}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


directory = '.\MEX-M-HRSC-3-RDR-EXT9-V4.0\\'

for filename in os.listdir(directory):
    file_path = os.path.join(directory, filename)
    
    # Check if it's a file (and not a directory)
    if os.path.isfile(file_path):
        insert_blob(file_path)


