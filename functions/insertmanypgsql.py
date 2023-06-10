import psycopg2
from params import postgresql

def insertmanypgsql(script: str, lista):

    try:
        host = postgresql['host']
        user = postgresql['user']
        password = postgresql['password']
        database = postgresql['database']

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