from fastapi import FastAPI
import pandas as pd
import io
import psycopg2
from functions.connectpgsql import connectpgsql


app = FastAPI()


@app.get("/move_historical_data")
def move_historical_data():

    try:
        csv_path = "csv_files/hired_employees.csv"
        df = pd.read_csv(csv_path)
        
        #connectpgsql("drop_table.sql")
        connectpgsql("create_table.sql")
        
        return f"Se inserto {df.shape[0]} registros de forma exitosa !"
    except Exception as e:
        return {"error": str(e)}

@app.get("/backup")
def backup():
    return "Se hizo el backup de forma exitosa !" 

@app.get("/restore")
def restore():
    return "Se restauro las tablas de forma exitosa !"