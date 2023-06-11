import psycopg2
from params import postgresql
from fastavro import writer, reader

def table_to_avro(script: str, table: str, filename: str):

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
        records = []

        if table == "employees":
            for row in rows:
                record = {
                    "id": row[0],
                    "name": row[1],
                    "datetime": row[2],
                    "department_id": row[3],
                    "job_id": row[4]
                }
                records.append(record)
            
            schema = {
                        "type": "record",
                        "name": "bronze.hired_employees",
                        "fields": [
                            {"name": "id", "type": "int"},
                            {"name": "name", "type": "string"},
                            {"name": "datetime", "type": "string"},
                            {"name": "department_id", "type": "int"},
                            {"name": "job_id", "type": "int"}
                        ]
                    }
        elif table == "departments":
            for row in rows:
                record = {
                    "id": row[0],
                    "department": row[1]
                }
                records.append(record)

            schema = {
                        "type": "record",
                        "name": "bronze.departments",
                        "fields": [
                            {"name": "id", "type": "int"},
                            {"name": "department", "type": "string"}
                        ]
                    }
        elif table == "jobs":
            for row in rows:
                record = {
                    "id": row[0],
                    "job": row[1]
                }
                records.append(record)

            schema = {
                        "type": "record",
                        "name": "bronze.jobs",
                        "fields": [
                            {"name": "id", "type": "int"},
                            {"name": "job", "type": "string"}
                        ]
                    }

        cursor.close()
        conn.close()

        with open(filename, "wb") as avro_file:
            writer(avro_file, schema , records=records)
        #print(records)

    except Exception as e:
        return {"error": str(e)}