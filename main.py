from fastapi import FastAPI
import pandas as pd
import io
import psycopg2
from functions.connectpgsql import connectpgsql
from functions.insertmanypgsql import insertmanypgsql
from params import tables

bach_size = 1000

app = FastAPI()

host = "localhost"
user = "postgres"
password = "postgres"
database = "postgres"

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

        print(df.head())
        print(f"Se inserto correctamente en la tabla {table['nombre']}")
    return f"Se inserto de forma exitosa !"

@app.get("/backup")
def backup():
    return "Se hizo el backup de forma exitosa !" 

@app.get("/restore")
def restore():
    return "Se restauro las tablas de forma exitosa !"