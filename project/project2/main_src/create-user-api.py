import sqlite3

conn = sqlite3.connect('users.db')
cursor = conn.cursor()

cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users';")
table_exists = cursor.fetchone()

if table_exists:
    print("Table 'users' already exists.")
else:
    cursor.execute('''CREATE TABLE users (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        clientID TEXT NOT NULL UNIQUE,
                        clientSecretKey TEXT NOT NULL
                    )''')
    print("Table 'users' created successfully.")


# Commit changes and close the connection
conn.commit()
conn.close()
