import psycopg2

conn = psycopg2.connect(database = 'netology_db', user='postgres', password='123098123Kol')

#Task1. Функция, создающая структуру БД (таблицы).
def create_table(cursor) -> None:
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS client(
        client_id SERIAL PRIMARY KEY,
        first_name VARCHAR(40),
        last_name VARCHAR(60),
        e_mail VARCHAR(60) NOT NULL UNIQUE,
        contact_number VARCHAR(10) []
    );
    """)
    conn.commit()
    print(f'Таблица client успешно создана.')

#Task2. Функция, позволяющая добавить нового клиента.
def add_client(cursor, first_name: str, last_name: str, e_mail: str, phone=None) -> None:
    cursor.execute("""INSERT INTO client(first_name, last_name, e_mail, contact_number)
    VALUES(%s, %s, %s, %s);""", (first_name, last_name, e_mail, phone))
    conn.commit()
    print(f'Клиент {first_name} {last_name} успешно добавлен.')

#Task3. Функция, позволяющая добавить телефон для существующего клиента.
def add_phone(cursor, client_id: int, new_phone: str):
    cursor.execute("""
    SELECT contact_number FROM client
    WHERE client_id = %s""", (client_id, ))
    phone = cursor.fetchone()
    if phone[0] == None:
        cursor.execute("""
        UPDATE client
        SET contact_number = %s
        WHERE client_id = %s
        """, ([new_phone], client_id))
    else:
        result  = phone[0]
        result.append(new_phone)
        cursor.execute("""
        UPDATE client
        SET contact_number = %s
        WHERE client_id = %s
        """, (result, client_id))
    conn.commit()
    print(f'Клиенту с номером {client_id} успешно добавлен телефон {new_phone}.')

#Task4.Функция, позволяющая изменить данные о клиенте.
def change_client(cursor, client_id, first_name=None, last_name=None, email=None, phones=None):
    my_dict = {
        'first_name': first_name,
        'last_name': last_name,
        'e_mail': email,
        'contact_numbers': phones
    }
    for key, value in my_dict.items():
        if value:
            cursor.execute("""
            UPDATE client
            SET {} = %s
            WHERE client_id = %s
            """.format(key), (value, client_id))
            print(f'У клиента с номером {client_id} изменено поле {key} на {value}')
    conn.commit()

#Task5. Функция, позволяющая удалить телефон для существующего клиента.
def delete_phone(cursor, client_id, phone):
    cursor.execute("""
    SELECT contact_number
    FROM client
    WHERE client_id = %s""", (client_id,))
    numbers = cursor.fetchone()
    if not numbers[0] or phone not in numbers[0]:
        print(f'У клиента с номером {client_id} отсутствует указанный номер телефона.')
    else:
        result = numbers[0]
        result.remove(phone)
        if not result:
            result = None
        cursor.execute("""
        UPDATE client
        SET contact_number = %s
        WHERE  client_id = %s""", (result, client_id))
        print(f'Абонентский номер {phone} удален у клиента с номером {client_id}')
    conn.commit()

#Task6. Функция, позволяющая удалить существующего клиента.
def delete_client(cursor, client_id):
    cursor.execute("""
    SELECT client_id FROM client""")
    id = cursor.fetchall()
    find = 0
    for i in id:
        if client_id in i:
            find+=1
            cursor.execute("""
            DELETE FROM client
            WHERE client_id = %s""", (i[0],))
    conn.commit()
    if find == 0:
        print(f'Клиент с номером {client_id} отсутствует в базе данных.')
    else:
        print(f'Клиент с номером {client_id} удален из базы данных')

#Task7. Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону.
def find_client(cursor, first_name=None, last_name=None, email=None, phone=None):
    if first_name == None and last_name == None and email == None and phone == None:
        print('Не выбраны параметры для поиска')
    elif first_name != None and last_name != None: #Поиск по имени и фамилии
        cursor.execute("""
        SELECT * FROM client
        WHERE first_name = %s AND last_name = %s""", (first_name, last_name))
        result = cursor.fetchall()
        if not result:
            print('Клиент отсутствует в базе данных')
        else:
            print(result)
    elif email: # Поиск по email, так как данное поле должно быть уникальным.
        cursor.execute("""
        SELECT * FROM client
        WHERE e_mail = %s""", (email, ))
        result = cursor.fetchone()
        if not result:
            print('Клиент отсутствует в базе данных')
        else:
            print(result)
    elif phone: #Поиск клиента по номеру телефону
        cursor.execute("""
        SELECT * FROM client
        WHERE ARRAY_POSITION(contact_number, %s) IS NOT NULL""", (phone,))
        result = cursor.fetchone()
        if result:
            print(result)
        else:
            print('Клиент с указанным номером телефона отсутствует в базе данных')

#Проверка работоспособности функций.
with conn.cursor() as cur:
    cur.execute("""
    DROP TABLE client;
    """)
    conn.commit()

    create_table(cur)
    add_client(cur, 'Adam', 'Sendler', 'AdamSendler@mail.ru')
    add_client(cur, 'Ben', 'Aflek', 'BenAflek@mail.ru', ["9273333333"])
    add_client(cur, 'Chak', 'Noris', 'ChakNoris@mail.ru', ["9277777777", "9371111111"])
    add_client(cur, 'Stiven', 'Sigal', 'StivenSigal@mail.ru', ["9275555555"])
    print('*' * 40)

    add_phone(cur, 2, '9272222222')
    cur.execute("""SELECT first_name, last_name, e_mail, contact_number
            FROM client WHERE client_id = 2""")
    print(cur.fetchone())
    print('*'*40)

    change_client(cur, 2, first_name='James')
    change_client(cur, 2, last_name='Blant', email='JamesBlant@mail.ru')
    cur.execute("""SELECT first_name, last_name, e_mail, contact_number
            FROM client WHERE client_id = 2""")
    print(cur.fetchone())
    print('*'*40)

    delete_phone(cur, 4, '9275555555')
    delete_phone(cur, 2, '9273333333')
    delete_phone(cur, 3, '9277777777')
    cur.execute("""SELECT first_name, last_name, e_mail, contact_number
        FROM client WHERE client_id = 4""")
    print(cur.fetchone())
    delete_phone(cur, 3, '12345')
    print('*' * 40)

    delete_client(cur, 2)
    delete_client(cur, 5)
    print('*'*40)

    find_client(cur)
    find_client(cur, first_name='Stiven', last_name='Sigal')
    find_client(cur, first_name='Stiv', last_name='Sigal')
    find_client(cur, email='ChakNoris@mail.ru')
    find_client(cur, email='sfvdfbdfb@mail.ru')
    find_client(cur, phone='9371111111')
    find_client(cur, phone='11111')

conn.close()