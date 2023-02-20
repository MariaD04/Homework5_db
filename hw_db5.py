import psycopg2

def create_db(conn):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS Client(
        client_id SERIAL PRIMARY KEY,
        name VARCHAR(60) NOT NULL,
        surname VARCHAR(60) NOT NULL,
        email VARCHAR(60) NOT NULL UNIQUE);
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS Phone(
        number BIGINT UNIQUE,
        client_id INTEGER NOT NULL REFERENCES Client(client_id));
    """)

def insert_client(conn, name, surname, email, number=None):
    conn.execute("""
    INSERT INTO Client(name, surname, email)
    VALUES(%s, %s, %s)
    RETURNING client_id, name, surname, email;
    """,(name, surname, email))
    return(conn.fetchall())

def insert_phone(conn, number, client_id):
    conn.execute("""
    INSERT INTO Phone(number, client_id)
    VALUES(%s, %s)
    RETURNING number, client_id;
    """,(number, client_id))
    return(conn.fetchall())

def update_data(conn, client_id, name=None, surname=None, email=None, number=None):
    conn.execute("""
    UPDATE Client SET name = %s, surname = %s, email = %s
    WHERE client_id = %s 
    RETURNING client_id, name, surname, email;
    """, (name, surname, email, client_id))

    conn.execute("""
    UPDATE Phone SET number = %s
    WHERE client_id = %s AND number = (
        SELECT number FROM Phone p
        LEFT JOIN Client c ON c.client_id = p.client_id
        GROUP BY p.number, c.client_id
        HAVING c.client_id = %s
        ORDER BY number
        LIMIT 1)
    RETURNING client_id, number;
    """, (number, client_id, client_id))

def delete_phone(conn, client_id, number):
    conn.execute("""
    DELETE FROM Phone WHERE client_id = %s AND number = %s;
    """, (client_id, number))

def delete_client(conn, client_id):
    conn.execute("""
    DELETE FROM Client WHERE client_id = %s;
    """, (client_id))

def find_client(conn, name=None, surname=None, email=None, number=None):
    conn.execute("""
    SELECT c.name, c.surname, c.email, p.number FROM Client c
    LEFT JOIN Phone p ON c.client_id = p.client_id
    WHERE c.name = %s OR c.surname = %s OR c.email = %s OR p.number = %s;
    """,(name, surname, email, number))
    return(conn.fetchall())

conn = psycopg2.connect(database = 'db', user = 'postgres', password = 'postgresdb')
with conn.cursor() as cur:
    cur.execute("""
        DROP TABLE Phone;
        DROP TABLE Client;
        """)
    create_db(cur)
    conn.commit()
    print('Результат после добавления данных')
    print(insert_client(cur, 'Ivan', 'Ivanov', 'ivanovivan@mail.ru'))
    print(insert_client(cur, 'Sasha', 'Petrov', 'sashapetrov@mail.ru'))
    conn.commit()
    print(insert_phone(cur, '89276352415', '1'))
    print(insert_phone(cur, '89275632415', '2'))
    print(insert_phone(cur, '89275632417', '2'))
    conn.commit()
    print('Результат после изменения данных')
    update_data(cur, '1', 'Andrey',  'Ivanov', 'ivanovandrey@mail.ru', '89277594735')
    update_data(cur, '2', 'Sasha', 'Petrov', 'sashapetrov@mail.ru', '89277594739')
    print(find_client(cur, 'Andrey'))
    print(find_client(cur, 'Sasha'))
    conn.commit()
    delete_phone(cur, '1', '89277594735')
    delete_client(cur, '1')
    conn.commit()
    print('Результат после поиска клиента')
    print(find_client(cur, 'Sasha'))
    conn.commit()
     
conn.close()