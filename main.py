from fastapi import FastAPI
import pandas as pd
import io
import psycopg2
from functions.connectpgsql import connectpgsql
from functions.insertmanypgsql import insertmanypgsql
from functions.table_to_avro import table_to_avro
from params import tables
from params import postgresql
from params import params
from fastavro import writer, reader
import datetime
import csv

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

            print(values)
        
        print(f"Se inserto correctamente los datos en la tabla {table['nombre']}")
    return f"Se inserto de forma exitosa !"

@app.get("/backup") 
def backup():
    
    for table in tables:

        backup_filename = f"avro_files/backup_{table['nombre']}.avro"
        table_postgre = f"{table['nombre']}"
        query = f"select_{table['nombre']}.sql"

        table_to_avro(query,table_postgre,backup_filename)

        print(f"Se hizo el backup de forma exitosa de la tabla {table['nombre']}!")
    return f"Se hizo el backup de forma exitosa de las tablas"

@app.get("/restore/{tabla}")
def restore(tabla):
    #tabla = "jobs"
    try:
        backup_filename = f"avro_files/backup_{tabla}.avro"
        with open(backup_filename, "rb") as avro_file:
            r = reader(avro_file)
            records = list(r)
        connectpgsql(f"create_{tabla}_backup.sql")

        lista = []
        
        if tabla == "employees":
            for record in records:
                tupla = (record['id'], record['name'], record['datetime'], record['department_id'], record['job_id'])
                lista.append(tupla)

        elif tabla == "departments":
            for record in records:
                tupla = (record['id'], record['department'])
                lista.append(tupla)

        elif tabla == "jobs":
            for record in records:
                tupla = (record['id'], record['job'])
                lista.append(tupla)

        insertmanypgsql(f"insert_{tabla}_backup.sql",lista)

        print(f"Restauración de la tabla {tabla} completada correctamente.")

    except Exception as e:
        print(f"Error al leer el archivo Avro: {str(e)}")
    return "Se restauro las tablas de forma exitosa !"


#App de prueba para revisar si los datos fueron exportados correctamente
@app.get("/leeravro")
def restore():

    try:
        backup_filename = "avro_files/backup_jobs.avro"
        with open(backup_filename, "rb") as avro_file:
            r = reader(avro_file)
            for record in r:
                print(record)

    except Exception as e:
        print(f"Error al leer el archivo Avro: {str(e)}")
    
    return "Se leyo el archivo avro de forma exitosa !"

@app.get("/employees_by_dep_job")
def restore():

    try:
        conn = psycopg2.connect(host=host, user=user, password=password, database=database)
        cur = conn.cursor()

        select_query = """
            select b.department,c.job,
            count(CASE WHEN DATE_TRUNC('quarter', a.datetime::date) = DATE_TRUNC('quarter', '2021-01-01'::date) THEN 1 ELSE 0 END) AS Q1,
            count(CASE WHEN DATE_TRUNC('quarter', a.datetime::date) = DATE_TRUNC('quarter', '2021-04-01'::date) THEN 1 ELSE 0 END) AS Q2,
            count(CASE WHEN DATE_TRUNC('quarter', a.datetime::date) = DATE_TRUNC('quarter', '2021-07-01'::date) THEN 1 ELSE 0 END) AS Q3,
            count(CASE WHEN DATE_TRUNC('quarter', a.datetime::date) = DATE_TRUNC('quarter', '2021-10-01'::date) THEN 1 ELSE 0 END) AS Q4

            from bronze.hired_employees a
            left join bronze.departments b on a.department_id = b.id
            left join bronze.jobs c on a.job_id = c.id
            group by 1,2 order by 1,2
        """
        cur.execute(select_query)
        rows = cur.fetchall()

        headers = [desc[0] for desc in cur.description]

        with open('csv_files/employees_by_dep_job.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            
            writer.writerow(headers)
            writer.writerows(rows)

        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error al ejecutar la consulta: {str(e)}")

    return "Reporte exportado en csv realizado correctamente !"


@app.get("/employees_by_department")
def restore():

    try:
        conn = psycopg2.connect(host=host, user=user, password=password, database=database)
        cur = conn.cursor()

        select_query = """
            select b.id, b.department, count(1) q
                from bronze.hired_employees a
            left join bronze.departments b on a.department_id = b.id
            left join bronze.jobs c on a.job_id = c.id
            group by 1,2
                having count(1) >= (select avg(a.q)
                from (
            select department_id, count(1) q from bronze.hired_employees a where DATE_TRUNC('year', a.datetime::date) = DATE_TRUNC('year', '2021-01-01'::date)
            group by 1) a )
            order by count(1) desc,1
        """
        cur.execute(select_query)
        rows = cur.fetchall()

        headers = [desc[0] for desc in cur.description]

        with open('csv_files/employees_by_department.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            
            writer.writerow(headers)
            writer.writerows(rows)

        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error al ejecutar la consulta: {str(e)}")

    return "Reporte exportado en csv realizado correctamente !"
