from fastapi import FastAPI
import pandas as pd
import io
import psycopg2
from functions.connectpgsql import connectpgsql
from functions.insertmanypgsql import insertmanypgsql
from params import tables
from params import postgresql
from params import params
from fastavro import writer, reader
import datetime

bach_size = params['bach_size']

app = FastAPI()

host = postgresql['host']
user = postgresql['user']
password = postgresql['password']
database = postgresql['database']

@app.get("/move_historical_data")
def move_historical_data():

    for table in tables:

        csv_path = f"csv_files/{table['nombre']}.csv"
        df = pd.read_csv(csv_path ,header=None)
        df_nulos = df[df.isnull().any(axis=1)]
        df_valido = df.dropna()

        #connectpgsql(f"drop_{table['nombre']}.sql")
        connectpgsql(f"create_{table['nombre']}.sql")


        for start in range(0,df_valido.shape[0], bach_size):
            
            batch_df = df_valido.iloc[start:start+bach_size]
            values = [tuple(row) for row in batch_df.values]

            insertmanypgsql(f"insert_{table['nombre']}.sql",values)

        #print(df.head())
        
        print(f"Se inserto correctamente los datos en la tabla {table['nombre']}")
    return f"Se inserto de forma exitosa !"

@app.get("/backup")
def backup():
    try:
        backup_filename = f"avro_files/backup_employees.avro"

        query = " select * from bronze.hired_employees where id <= 10"

        conn = psycopg2.connect(host = host,user = user,password = password,database = database)
        cur= conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        records = []
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
        with open(backup_filename, "wb") as avro_file:
            writer(avro_file, schema , records=records)
        print(records)

    except Exception as e:
        return {"error": str(e)} 
    
    return f"Se hizo el backup de forma exitosa en {backup_filename}!" 

@app.get("/restore")
def restore():
    return "Se restauro las tablas de forma exitosa !"


@app.get("/leeravro")
def restore():

    try:
        backup_filename = "avro_files/backup_employees.avro"
        with open(backup_filename, "rb") as avro_file:
            r = reader(avro_file)
            for record in r:
                print(record)

    except Exception as e:
        print(f"Error al leer el archivo Avro: {str(e)}")
    
    return "Se leyo el archivo avro de forma exitosa !"