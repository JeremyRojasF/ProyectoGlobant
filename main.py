from fastapi import FastAPI

app = FastAPI()


@app.get("/move_historical_data")
def move_historical_data():
    return "Se inserto la data historica de forma exitosa !"

@app.get("/backup")
def backup():
    return "Se hizo el backup de forma exitosa !"

@app.get("/restore")
def restore():
    return "Se restauro las tablas de forma exitosa !"