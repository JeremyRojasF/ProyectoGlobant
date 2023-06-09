from fastapi import FastAPI
import pandas as pd
import io
import psycopg2
from functions.connectpgsql import connectpgsql

bach_size = 1000

app = FastAPI()

host = "localhost"
user = "postgres"
password = "postgres"
database = "postgres"

@app.get("/move_historical_data")
def move_historical_data():


    csv_path = "csv_files/hired_employees_test.csv"
    df = pd.read_csv(csv_path ,header=None)
    df_nulos = df[df.isnull().any(axis=1)]
    df_valido = df.dropna()

    df_valido[1] = df_valido[1].astype(str)
    df_valido[2] = df_valido[2].astype(str)
    df_valido[3] = df_valido[3].astype(int)
    df_valido[4] = df_valido[4].astype(int)
    
    connectpgsql("drop_table.sql")
    connectpgsql("create_table.sql")


    for start in range(0,df.shape[0], bach_size):
        batch_df = df.iloc[start:start+bach_size]

        values = [tuple(row) for row in batch_df.values]
        insert_query = """
            INSERT INTO bronze.hired_employees (id, name, datetime, department_id, job_id)
            VALUES (%s,%s,%s,%s,%s);
        """
        conn = psycopg2.connect(host = host,user = user,password = password,database = database)
        cursor = conn.cursor()
        cursor.executemany(insert_query, values)
        conn.commit()
        cursor.close()
        conn.close()

    # print(df.head())
    # print(df_nulos.head())
    # print(df_valido.head())
    # print(df.info())
    print(values)
    #print(df_nulos.info())
    #print(df_valido.info())
    return f"Se inserto {df.shape[0]} registros de forma exitosa !"

@app.get("/backup")
def backup():
    return "Se hizo el backup de forma exitosa !" 

@app.get("/restore")
def restore():
    return "Se restauro las tablas de forma exitosa !"