import json
import sqlite3
    
def retrieve_data_from_sqlite(database_file, table_name, chunk_size, random=False):
    data_dict = {}
    try:
        # Connect to the SQLite database.
        conn = sqlite3.connect(database_file)
        cursor = conn.cursor()

        # Retrieve data from the database.
        if random:
            cursor.execute(f"SELECT rowid FROM {table_name} ORDER BY RANDOM() LIMIT ?", (chunk_size, ))
            random_row_ids = [row[0] for row in cursor.fetchall()]
            cursor.execute("SELECT key, value FROM {} WHERE rowid IN ({})".format(table_name, ','.join(map(str, random_row_ids))))
        else:
            cursor.execute(f"SELECT key, value FROM {table_name} LIMIT ?", (chunk_size, ))  

        for key, value_json in cursor.fetchall():
            # Deserialize the JSON back to a list
            value_list = json.loads(value_json)
            data_dict[key] = value_list
        
        print("Data retrieved successfully.")
    
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the cursor and connection in the finally block.
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return data_dict