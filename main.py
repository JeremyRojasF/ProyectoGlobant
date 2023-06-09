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


    csv_path = "csv_files/employees.csv"
    df = pd.read_csv(csv_path ,header=None)
    df_nulos = df[df.isnull().any(axis=1)]
    df_valido = df.dropna()

    df_valido[1] = df_valido[1].astype(str)
    df_valido[2] = df_valido[2].astype(str)
    df_valido[3] = df_valido[3].astype(int)
    df_valido[4] = df_valido[4].astype(int)

    connectpgsql("drop_employees.sql")
    connectpgsql("create_employees.sql")


    for start in range(0,df_valido.shape[0], bach_size):
        
        batch_df = df_valido.iloc[start:start+bach_size]
        values = [tuple(row) for row in batch_df.values]

        insertmanypgsql("insert_employees.sql",values)

    print(df_nulos.head())
    return f"Se inserto {df.shape[0]} registros de forma exitosa !"

@app.get("/backup")
def backup():
    return "Se hizo el backup de forma exitosa !" 

@app.get("/restore")
def restore():
    return "Se restauro las tablas de forma exitosa !"