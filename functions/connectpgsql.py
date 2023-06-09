import psycopg2

def connectpgsql(script: str):

    host = "localhost"
    user = "postgres"
    password = "postgres"
    database = "postgres"

    with open(f'sql/{script}', 'r') as file:
        query = file.read()

    conn = psycopg2.connect(host = host,user = user,password = password,database = database)
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()