import psycopg2
from params import postgresql
import csv
def table_to_csv(script: str, filename: str):

    try:
        host = postgresql['host']
        user = postgresql['user']
        password = postgresql['password']
        database = postgresql['database']

        with open(f'sql/{script}', 'r') as file:
            query = file.read()

        conn = psycopg2.connect(host = host,user = user,password = password,database = database)
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()

        rows = cursor.fetchall()
        headers = [desc[0] for desc in cursor.description]

        with open(f'csv_files/{filename}.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            
            writer.writerow(headers)
            writer.writerows(rows)

        cursor.close()
        conn.close()

    except Exception as e:
        return {"error": str(e)}