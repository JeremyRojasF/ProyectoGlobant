import psycopg2

def insertmanypgsql(script: str, lista):

    try:
        host = "localhost"
        user = "postgres"
        password = "postgres"
        database = "postgres"

        with open(f'sql/{script}', 'r') as file:
            query = file.read()

        conn = psycopg2.connect(host = host,user = user,password = password,database = database)
        cursor = conn.cursor()
        cursor.executemany(query, lista)
        conn.commit()
        cursor.close()
        conn.close()

        return f"Se insertaron correctamente los registros !"
    except Exception as e:
        return {"error": str(e)}