from fastapi import FastAPI
import pandas as pd
import io
app = FastAPI()


@app.get("/move_historical_data")
def move_historical_data():
    csv_path = "csv_files/hired_employees.csv"
    df = pd.read_csv(csv_path)
    
    with open('sql/create_table.sql', 'r') as file:
        create_table = file.read()

    return f"Se inserto {df.shape[0]} registros de forma exitosa !" + create_table

@app.get("/backup")
def backup():
    return "Se hizo el backup de forma exitosa !" 

@app.get("/restore")
def restore():
    return "Se restauro las tablas de forma exitosa !"