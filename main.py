from fastapi import FastAPI
import pandas as pd
import io
import psycopg2

host = "localhost"
user = "postgres"
password = "postgres"
database = "postgres"

app = FastAPI()


@app.get("/move_historical_data")
def move_historical_data():

    try:
        csv_path = "csv_files/hired_employees.csv"
        df = pd.read_csv(csv_path)
        
        with open('sql/create_table.sql', 'r') as file:
            create_table = file.read()



        conn = psycopg2.connect(host = host,user = user,password = password,database = database)
        cursor = conn.cursor()
        cursor.execute("select version()" )
        row = cursor.fetchone()
        #return f"Se inserto {df.shape[0]} registros de forma exitosa !"
        return row
    except Exception as e:
        return {"error": str(e)}

@app.get("/backup")
def backup():
    return "Se hizo el backup de forma exitosa !" 

@app.get("/restore")
def restore():
    return "Se restauro las tablas de forma exitosa !"